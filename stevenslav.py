import datetime
import requests
import json
import logging
from logging.handlers import RotatingFileHandler
import time
import requests
import sys
import base64
import sys

from tqdm import tqdm
from cryptography.fernet import Fernet, MultiFernet

from db_connections import Connection_Manager
import construct_env

from google.cloud import secretmanager


secret_manager_client = secretmanager.SecretManagerServiceClient()

keys = secret_manager_client.access_secret_version(
    name=f"projects/eql-data-processing/secrets/pave-agent-decryption-keys/versions/latest"
).payload.data.decode("UTF-8")
keys = json.loads(keys)["KEYS"]


def base64_decode(val: str) -> bytes:
    return base64.urlsafe_b64decode(val.encode("ascii"))


def decrypt(val: str) -> str:
    fernet = MultiFernet(Fernet(k) for k in keys)
    actual = fernet.decrypt(base64_decode(val))
    return actual.decode()


def handle_pave_request(
    user_id: str,
    method: str,
    endpoint: str,
    payload: dict,
    headers: dict,
    params: dict,
    last_wait: float = 0,
):
    logging.info(
        "user_id:{}\n\tRequest: {} /{}".format(user_id, method.upper(), endpoint)
    )

    if method == "get":
        res = requests.get(f"{endpoint}", json=payload, headers=headers, params=params)
    elif method == "post":
        res = requests.post(f"{endpoint}", json=payload, headers=headers, params=params)
    else:
        raise ValueError("Method not understood {}".format(method))

    res_code = res.status_code
    res_json = json.dumps(res.json())
    logging.info(
        f"\tResponse: {res_code} \n{res_json[:100]}...{res_json[-100:]}"
    )

    if res_code == 429:
        sleep = 1 if last_wait == 0 else last_wait * 2
        logging.error(f"Request limit reached, waiting {sleep} second(s)")
        time.sleep(sleep)
        return handle_pave_request(
            user_id, method, endpoint, payload, headers, params, sleep
        )
    else:
        return res


def insert_response_into_db(
    user_id: str, res, mongo_db, collection_name: str, response_column_name: str
):
    logging.info(
        "Inserting response into: {}.{}".format(collection_name, response_column_name)
    )
    mongo_collection = mongo_db[collection_name]
    res_code = res.status_code

    # TODO: For pave uploads, we replace the current record with this new one effectively having
    # a moving window of ten years of transactions. This sucks in terms of performance and safety
    # so we're going to have to chage this a little
    if res_code == 200:
        mongo_collection.replace_one(
            {"user_id": user_id},
            {
                response_column_name: res.json(),
                "user_id": user_id,
                "response_code": res.status_code,
                "date": datetime.datetime.now(),
            },
            upsert=True,
        )
    else:
        logging.warning("\tCan't insert: {} {}\n".format(res_code, res.json()))


def main():
    # Set up the logging instance
    logging.basicConfig(
        handlers=[
            RotatingFileHandler("/home/langston/pave-prism/stevenslav.log", maxBytes=1024**3, backupCount=2, mode="a")
        ],
        format="%(name)s @ %(asctime)s: %(message)s",
        datefmt="%I:%M:%S",
        level=logging.DEBUG,
    )

    cm = Connection_Manager()
    process_start = datetime.datetime.now()

    try:
        user_ids = []

        # Open connection to postgres db
        conn = cm.get_postgres_connection()

        # If a dummy argument is passed into the command, run evaluation for all users (we're contructing the env)
        # else run for user who have signed up in the past 2 hours
        if len(sys.argv) > 1:
            rows = conn.execute("SELECT DISTINCT id FROM public.users ").fetchall()
        else:
            rows = conn.execute(
                "SELECT DISTINCT id FROM public.users WHERE created_at >= (NOW() - INTERVAL '2 hours')"
            ).fetchall()
        user_ids = [str(row[0]) for row in rows]

        if len(user_ids) < 1:
            # Close db connection
            cm.close_postgres_connection(conn)

            process_end = datetime.datetime.now()
            logging.info(f"No new users found. Ending process")
            logging.info(f"\nTotal runtime: {process_end-process_start}")
            exit(0)

        # call the construnct env + docker run command when the dummy arg is passed
        # this gathers the users in user_ids and their access tokens, puts them in an env, and calls
        # a full upload pave-agent docker process which will upload all user transactions
        if len(sys.argv) > 1:
            logging.info("Starting to contruct env for all-user sync")
            finished = construct_env.run(user_ids)
            logging.info(f"Done with the construct env call {finished}") 

        # Get pave secret values
        pave_str = secret_manager_client.access_secret_version(
            name=f"projects/eql-data-processing/secrets/pave-prism-info/versions/latest"
        ).payload.data.decode("UTF-8")

        pave_data = json.loads(pave_str)
        pave_base_url = pave_data["PAVE_HOST"]
        pave_x_api_key = pave_data["PAVE_X_API_KEY"]
        pave_headers = {
            "Content-Type": "application/plaid+json",
            "x-api-key": pave_x_api_key,
        }

        # Establish connection to mongo db
        mongo_db = cm.get_pymongo_table("pave")

        logging.debug(f"Running evals for {len(user_ids)} users...\n")
        for user_id in tqdm(user_ids):
            logging.debug("Eval for user {}".format(user_id))

            # When the dummy argument was NOT passed in then we do ad hoc calls to a running pave docker image
            # This is run every two hours
            if len(sys.argv) == 1:
                # TODO: User could have multiple access tokens (i've seen it), probably loop through all and see which one works
                rows = conn.execute(
                    f"SELECT DISTINCT access_token FROM public.plaid_links WHERE user_id = '{user_id}'"
                ).fetchall()
                access_tokens = [decrypt(str(row[0])) for row in rows]

                # If we get here we assume that user at user_id is a new user with transactions in the backend
                # we used to pull directly from the db but now we can leverage the pave-agent docker image currently running
                for access_token in access_tokens:
                    res = requests.post(
                        f"http://127.0.0.1:8123/v1/users/{user_id}/upload?num_transaction_days=3650",
                        json={"access_token": f"{access_token}"},
                    )
                    res = res.json()

                    # if res.status_code != 200:
                    #    logging.error("Got bad response from pave {res.json()}"
                    logging.debug(f"Got response from pave-agent: {res}")

                # The requests to pave return immediately so I'll add some manual time so pave has a chance to process 3650 days worth of transactions
                time.sleep(10)
                # TODO: 10 seconds might be too much time but I've seen transaction processing take longer.
                #       We shouldn't have that may user to process to where this becomes an issue but it may
                #       cause unnecessary slowdowns

            # TODO: This is main slow down, pulling 10 years of transactions sucks especially when they are already on mongodb
            # find a way to cache what we've already stored and just add on today's data or 10 years based on that
            # Once the transactions are uploaded we can pull from pave as normal
            start_date_str = (
                datetime.datetime.now() - datetime.timedelta(days=365 * 10)
            ).strftime("%Y-%m-%d")
            end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d")

            # Store the balance data from pave
            params = {"start_date": start_date_str, "end_date": end_date_str}
            response = handle_pave_request(
                user_id=user_id,
                method="get",
                endpoint=f"{pave_base_url}/{user_id}/balances",
                payload=None,
                headers=pave_headers,
                params=params,
            )
            insert_response_into_db(
                user_id=user_id,
                res=response,
                mongo_db=mongo_db,
                collection_name="balances",
                response_column_name="balances",
            )
            #####################################################################

            # Store the transaction data from pave
            response = handle_pave_request(
                user_id=user_id,
                method="get",
                endpoint=f"{pave_base_url}/{user_id}/transactions",
                payload=None,
                headers=pave_headers,
                params=params,
            )
            insert_response_into_db(
                user_id=user_id,
                res=response,
                mongo_db=mongo_db,
                collection_name="transactions",
                response_column_name="transactions",
            )
            #####################################################################

            # Store the unified insights data from pave
            params = {
                "start_date": start_date_str,
                "end_date": end_date_str,
                "with_transactions": True,
            }
            response = handle_pave_request(
                user_id=user_id,
                method="get",
                endpoint=f"{pave_base_url}/{user_id}/unified_insights",
                payload=None,
                headers=pave_headers,
                params=params,
            )

            if response.status_code == 200:
                for title, object in response.json().items():
                    logging.info("Inserting response into: {}".format(title))
                    mongo_collection = mongo_db[title]
                    mongo_collection.replace_one(
                        {"user_id": user_id},
                        {
                            title: object,
                            "user_id": user_id,
                            "response_code": response.status_code,
                            "date": datetime.datetime.now(),
                        },
                        upsert=True,
                    )
            else:
                logging.warning("\tCan't insert: {} {}\n".format(response.status_code, response.json()))
            #####################################################################

            # Store the attribute data from pave
            params = {"date": end_date_str}
            response = handle_pave_request(
                user_id=user_id,
                method="get",
                endpoint=f"{pave_base_url}/{user_id}/attributes",
                payload=None,
                headers=pave_headers,
                params=params,
            )
            insert_response_into_db(
                user_id=user_id,
                res=response,
                mongo_db=mongo_db,
                collection_name="attributes",
                response_column_name="attributes",
            )
            #####################################################################
    except Exception as e:
        logging.exception(e)

    # Close db connections
    cm.close_pymongo_connection()
    cm.close_postgres_connection(conn)

    process_end = datetime.datetime.now()
    logging.info(f"\nTotal runtime: {process_end-process_start}")


if __name__ == "__main__":
    main()
