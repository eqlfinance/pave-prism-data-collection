import datetime
import sys
import requests
import json
import sqlalchemy
from google.cloud import secretmanager
from tqdm import tqdm
import logging
from db_connections import Connection_Manager
import time
import os


def aggregate_user_data(
    user_id: str,
    conn: sqlalchemy.engine.Connection,
    start_date_str: str = (
        datetime.datetime.now() - datetime.timedelta(days=365 * 10)
    ).strftime("%Y-%m-%d"),
    end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d"),
):
    # Set up the final objects that will be passed to pave
    account_data = {
        "run_timestamp": datetime.datetime.now().isoformat(),
        "accounts": [],
    }
    transaction_data = {
        "transactions": [],
        "start_date": start_date_str,
        "end_date": end_date_str,
    }

    # Get all plaid links that are owned by user_id, then find all the transactions associated with that link
    link_select_string = (
        f"SELECT id FROM public.plaid_links WHERE user_id = '{user_id}'"
    )
    link_id_rows = conn.execute(link_select_string).fetchall()

    sql_account_string = f"SELECT data FROM public.plaid_raw_transaction_sets "

    if len(link_id_rows) > 1:
        link_ids = tuple([str(x[0]) for x in link_id_rows])
        sql_account_string += f"WHERE link_id IN {link_ids}"
    elif len(link_id_rows) == 1:
        sql_account_string += f"WHERE link_id = '{(link_id_rows[0])[0]}'"
    else:
        logging.warning(f"\tNo links found for user {user_id}")
        return {"accounts": account_data, "transactions": transaction_data}

    sql_account_string += " ORDER BY created_at DESC"

    raw_transactions = conn.execute(sql_account_string).fetchall()

    # Collectors for the account and transaction ids to make sure there are no duplicates
    # Transactions are duplicated across raw_transaction_sets
    trans_ids = []
    account_ids = []

    for row in raw_transactions:
        for item in row[0]:
            for account in item["accounts"]:
                if account["account_id"] not in account_ids:
                    account_ids.append(account["account_id"])
                    account_data["accounts"].append(account)

            for transaction in item["transactions"]:
                if transaction["transaction_id"] not in trans_ids:
                    trans_ids.append(transaction["transaction_id"])

                    transaction_data["transactions"].append(
                        {
                            "transaction_id": transaction["transaction_id"],
                            "account_id": transaction["account_id"],
                            "amount": float(transaction["amount"]),
                            "date": transaction["date"],
                            "memo": " ".join(
                                transaction["personal_finance_category"].values()
                            )
                            if transaction["personal_finance_category"]
                            else "",
                            "name": transaction["name"] if transaction["name"] else " ",
                            "pending": transaction["pending"],
                            "category": transaction["category"],
                            "iso_currency_code": transaction["iso_currency_code"],
                            "authorized_date": transaction["authorized_date"],
                            "merchant_name": transaction["merchant_name"],
                            "payment_channel": transaction["payment_channel"],
                            "transaction_type": transaction["transaction_type"],
                        }
                    )

    return {"accounts": account_data, "transactions": transaction_data}


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
    logging.info(f"\tResponse: {res_code} {res.json()=}")

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
        filename="pave-eval-"
        + datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
        + ".log",
        format="%(name)s @ %(asctime)s: %(message)s",
        datefmt="%I:%M:%S",
        level=logging.DEBUG,
    )

    # Get pave secret values
    secret_manager_client = secretmanager.SecretManagerServiceClient()
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
    cm = Connection_Manager()
    mongo_db = cm.get_pymongo_table("pave")

    process_start = datetime.datetime.now()

    # Open connection to postgres db
    conn = cm.get_postgres_connection()

    # For testing: gets specific user ids from a provided env for ad hoc evals
    env_user_ids = os.getenv("PAVE_EVAL_USER_IDS", "")
    user_ids = []

    if env_user_ids != "":
        user_ids = env_user_ids.split(",")
        logging.debug(f"Running eval for {len(user_ids)} user(s)\n")
    else:
        # Calculate for all users
        rows = conn.execute("SELECT DISTINCT id FROM public.users").fetchall()
        user_ids = [str(row[0]) for row in rows]
        logging.debug("Running evals for all users...\n")

    # Loop through the users (default all of them) and run eval
    for user_id in tqdm(user_ids):
        logging.debug("Eval for user {}".format(user_id))

        # If this script is run with a dummy argument, the eval will post all transaction and balance data to pave
        if len(sys.argv) > 1:
            user_data = aggregate_user_data(user_id, conn)
            logging.info(
                f"\tAggregated data: {len(user_data['accounts']['accounts'])=}, {len(user_data['transactions']['transactions'])=}"
            )
            balance_data = user_data["accounts"]
            transaction_data = user_data["transactions"]

            if len(balance_data) == 0 or len(transaction_data["transactions"]) == 0:
                logging.warning(f"\tEmpty data for user {user_id}, skipping eval...")
                continue

            # Upload transaction data
            for i in range(0, len(transaction_data["transactions"]), 1000):
                trans = {"transactions": transaction_data["transactions"][i : i + 1000]}
                params = {
                    "resolve_duplicates": True,
                    "start_date": transaction_data["start_date"],
                    "end_date": transaction_data["end_date"],
                }
                response = handle_pave_request(
                    user_id=user_id,
                    method="post",
                    endpoint=f"{pave_base_url}/{user_id}/transactions",
                    payload=trans,
                    headers=pave_headers,
                    params=params,
                )
                insert_response_into_db(
                    user_id=user_id,
                    res=response,
                    mongo_db=mongo_db,
                    collection_name="transaction_uploads",
                    response_column_name="response",
                )

            # Upload balance data
            response = handle_pave_request(
                user_id=user_id,
                method="post",
                endpoint=f"{pave_base_url}/{user_id}/balances",
                payload=balance_data,
                headers=pave_headers,
                params=None,
            )
            insert_response_into_db(
                user_id=user_id,
                res=response,
                mongo_db=mongo_db,
                collection_name="balnace_uploads",
                response_column_name="response",
            )

        # These provide the date ranges for pave
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
            logging.warning(
                "\tCan't insert: {} {}\n".format(response.status_code, response.json())
            )
            

        # Store the attribute data from pave
        response = handle_pave_request(
            user_id=user_id,
            method="get",
            endpoint=f"{pave_base_url}/{user_id}/attributes",
            payload=None,
            headers=pave_headers,
            params=None,
        )

        insert_response_into_db(
            user_id=user_id,
            res=response,
            mongo_db=mongo_db,
            collection_name="attributes",
            response_column_name="attributes",
        )

    # Close db connections
    cm.close_pymongo_connection()
    cm.close_postgres_connection(conn)

    process_end = datetime.datetime.now()
    logging.info(f"\nTotal runtime: {process_end-process_start}")


if __name__ == "__main__":
    main()
