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

from google.cloud import secretmanager



# Set up the logging instance
logging.basicConfig(
    handlers=[
        RotatingFileHandler("stevenslav2.log", maxBytes=1024**3, backupCount=2, mode="a")
    ],
    format="%(name)s @ %(asctime)s: %(message)s",
    datefmt="%I:%M:%S",
    level=logging.DEBUG,
)

# Get pave secret values
secret_manager_client = secretmanager.SecretManagerServiceClient()

pave_table = "pave-stage-test"

# Decrpytion keys
keys = secret_manager_client.access_secret_version(
    name=f"projects/eql-data-processing-stage/secrets/pave-agent-decryption-keys/versions/latest"
).payload.data.decode("UTF-8")
keys = json.loads(keys)["KEYS"]

# Pave url necessities
pave_str = secret_manager_client.access_secret_version(
    name=f"projects/eql-data-processing-stage/secrets/pave-prism-info/versions/latest"
).payload.data.decode("UTF-8")

pave_data = json.loads(pave_str)
pave_base_url = pave_data["PAVE_HOST"]
pave_x_api_key = pave_data["PAVE_X_API_KEY"]
pave_headers = {
    "Content-Type": "application/plaid+json",
    "x-api-key": pave_x_api_key,
}


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
) -> requests.Response:
    logging.info(
        "user_id:{}\n\tRequest: {} /{}".format(user_id, method.upper(), endpoint)
    )

    request_timer = datetime.datetime.now()
     
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
        request_timer_end = datetime.datetime.now()
        logging.info(f"  DB insertion took: {request_timer_end-request_timer}")
        
        return res
    
def insert_response_into_db(
    user_id: str, res, mongo_db, collection_name: str, response_column_name: str
):
    logging.info(
        "Inserting response into: {}.{}".format(collection_name, response_column_name)
    )
    mongo_timer = datetime.datetime.now()
    mongo_collection = mongo_db[collection_name]
    res_code = res.status_code
    
    if res_code == 200:
        mongo_collection.insert_one(
            {
                response_column_name: res.json(),
                "user_id": user_id,
                "response_code": res.status_code,
                "date": datetime.datetime.now(),
            },
        )
    else:
        logging.warning("\tCan't insert: {} {}\n".format(res_code, res.json()))
    
    mongo_timer_end = datetime.datetime.now()
    logging.info(f"  DB insertion took: {mongo_timer_end-mongo_timer}")
    

'''
    Ran every 30 minutes
'''
def new_user_sync():
    logging.info("\nRuninng new user sync:\n")
    cm = Connection_Manager()
    
    # Open connection to postgres db
    conn = cm.get_postgres_connection()
    
    rows = conn.execute(
        "SELECT DISTINCT id FROM public.users WHERE created_at >= (NOW() - INTERVAL '30 minute')"
    ).fetchall()
    
    user_ids = [str(row[0]) for row in rows]

    # Get all user access tokens and upload transaction/balance them using the pave agent
    for user_id in tqdm(user_ids):
        rows = conn.execute(
            f"SELECT DISTINCT access_token FROM public.plaid_links WHERE user_id = '{user_id}'"
        ).fetchall()
        
        access_tokens = [decrypt(str(row[0])) for row in rows]

        for access_token in access_tokens:
            res = requests.post(
                f"http://127.0.0.1:8123/v1/users/{user_id}/upload?num_transaction_days=3650",
                json={"access_token": f"{access_token}"},
            )
            res = res.json()
            logging.debug(f"Got response from pave-agent: {res}")
            
            # Give pave agent some time to process transactions
            time.sleep(2)

        # Date ranges for pave
        start_date_str = (
            datetime.datetime.now() - datetime.timedelta(days=365 * 5)
        ).strftime("%Y-%m-%d")
        end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d")
        params = {"start_date": start_date_str, "end_date": end_date_str}
    
        # Get all transactions and upload them to mongodb
        # TODO: switch to pave-stage
        mongo_db = cm.get_pymongo_table(pave_table)

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
            

def hourly_sync():
    logging.info("\nRuninng Hourly Sync:\n")
    
    rows = conn.execute(
        "SELECT * FROM public.plaid_transactions WHERE plaid_transactions.date >= (NOW() - INTERVAL '25 days') LIMIT 1"
    ).fetchall()

    for row in rows:
        row = row._asdict()
        
        user_id = conn.execute(f"SELECT user_id FROM public.plaid_links WHERE id = \'{str(row['link_id'])}\'").fetchone()[0]
        
         # Date ranges for pave
        start_date_str = (
            datetime.datetime.now() - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d")
        end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d")
        params = {"start_date": start_date_str, "end_date": end_date_str, "resolve_duplicates": True}
        
        response = handle_pave_request(
            user_id=user_id,
            method="post",
            endpoint=f"{pave_base_url}/{user_id}/transactions",
            payload={"transactions": [row]},
            headers=pave_headers,
            params=params,
        )
        #####################################################################

        if response.status_code == 200:
            # TODO: switch to pave-stage
            mongo_db = cm.get_pymongo_table(pave_table)
            
            # Store the transaction data from pave
            response = handle_pave_request(
                user_id=user_id,
                method="get",
                endpoint=f"{pave_base_url}/{user_id}/transactions",
                payload=None,
                headers=pave_headers,
                params=params,
            )
            
            logging.info("Inserting response into transactions")
            mongo_timer = datetime.datetime.now()
            mongo_collection = mongo_db["transactions"]
            
            mongo_collection.update_one(
                {"user_id": user_id},
                {
                    "$push": {"$push": {"transactions.transactions": response.json()["transaction"]}},
                    "$currentDate": {"transactions.to": True, "date": False}
                }
            )
            
            mongo_timer_end = datetime.datetime.now()
            logging.info(f"  DB insertion took: {mongo_timer_end-mongo_timer}")
        else:
            logging.error("Could not upload transaction to mongodb") 
        #####################################################################
 
# Open connection to postgres db
cm = Connection_Manager()
conn = cm.get_postgres_connection() 

if __name__ == "__main__":
    process_start = datetime.datetime.now()
    try:
        which = sys.argv[1]
        
        if which == "new":
            new_user_sync()
        elif which == "hourly":
            hourly_sync()
        else:
            raise Exception("You must provide either new_users, hourly, or daily")
    except Exception as e:
        print(e)
        logging.exception(e)
    
    cm.close_pymongo_connection()
    cm.close_postgres_connection(conn)
    process_end = datetime.datetime.now()
    logging.info(f"\nTotal runtime: {process_end-process_start}")