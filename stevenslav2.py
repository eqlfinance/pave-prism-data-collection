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
from uuid import UUID

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

# TODO: switch to pave-stage
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
    if not val:
        return None
    
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
        f"\tResponse: {res_code} \n{res_json[:100]} ... {res_json[-100:]}"
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
        logging.info(f"  Pave request to {endpoint} took: {request_timer_end-request_timer}")
        
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
        mongo_collection.replace_one(
            {"user_id": user_id},
            {
                response_column_name: res.json(),
                "user_id": user_id,
                "response_code": res.status_code,
                "date": datetime.datetime.now(),
            },
            upsert=True
        )
    else:
        logging.warning("\tCan't insert to {}: {} {}\n".format(collection_name, res_code, res.json()))
    
    mongo_timer_end = datetime.datetime.now()
    logging.info(f"  DB insertion to {collection_name} took: {mongo_timer_end-mongo_timer}")
    

'''
    Ran every 30 minutes
'''
def new_link_sync():
    logging.info("\nRuninng new link sync:\n")
    cm = Connection_Manager()
    
    # Open connection to postgres db
    conn = cm.get_postgres_connection()
    
    rows = conn.execute(
        #"SELECT DISTINCT access_token, user_id FROM public.plaid_links WHERE created_at >= (NOW() - INTERVAL '30 minutes')"
        "SELECT DISTINCT access_token, user_id FROM public.plaid_links" 
    ).fetchall()
    
    for row in tqdm(rows):
        row = row._asdict()
        access_token, user_id = decrypt(row["access_token"]), str(row["user_id"])
        time_in_days = 365 * 2
        
        res = requests.post(
            f"http://127.0.0.1:8123/v1/users/{user_id}/upload?num_transaction_days={time_in_days}",
            json={"access_token": f"{access_token}"},
        )
        res = res.json()
        logging.debug(f"Got response from pave-agent: {res}")
        
        # Give pave agent some time to process transactions
        time.sleep(2)

        # Date ranges for pave
        start_date_str = (
            datetime.datetime.now() - datetime.timedelta(days=time_in_days)
        ).strftime("%Y-%m-%d")
        end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d")
        params = {"start_date": start_date_str, "end_date": end_date_str}
    
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
        
        mongo_timer = datetime.datetime.now()
        mongo_collection = mongo_db["transactions"]
        transactions = response.json()["transactions"]

        if len(transactions) > 0:
            logging.info(f"Inserting {json.dumps(transactions)[:200]}... into transactions")
           
            # Commenting out upsertion code because this user will be picked up by new_user_sync
            mongo_collection.update_one(
                {"user_id": str(user_id)},
                {
                    "$addToSet": {"transactions.transactions": {"$each": transactions}},
                    #"$setOnInsert": {"transactions": transactions, "user_id": user_id, "date"}
                    "$set": {"transactions.to": end_date_str, "date": datetime.datetime.now()}
                },
                #upsert=True
            )
            
            mongo_timer_end = datetime.datetime.now()
            logging.info(f"  DB insertion took: {mongo_timer_end-mongo_timer}")
        else:
            logging.warning("Got to new link transaction db insertion but no transactions were found for the date range")
        
        if response.status_code == 200:
            transactions = response.json()["transactions"]
            transaction_date_str = transactions[len(transactions)-1]["date"]
            logging.info(f" > Earliest transaction: {transaction_date_str}")
            params["start_date"] = transaction_date_str
        #####################################################################
        
        # Store the transaction data from pave
        response = handle_pave_request(
            user_id=user_id,
            method="get",
            endpoint=f"{pave_base_url}/{user_id}/balances",
            payload=None,
            headers=pave_headers,
            params=params,
        )
        
        mongo_timer = datetime.datetime.now()
        mongo_collection = mongo_db["balances"]
        balances = response.json()["accounts_balances"]


        if len(balances) > 0:
            logging.info(f"Inserting {json.dumps(balances)[:200]} into balances")

            mongo_collection.update_one(
                {"user_id": str(user_id)},
                {"$set": {"balances.to": end_date_str, "date": datetime.datetime.now()}}
            )
            for balance in balances:
                mongo_collection.update_one(
                    {"user_id": str(user_id), "balances.accounts_balances": {"$elemMatch": {"account_id": balance["account_id"]}}},
                    {"$addToSet": {"balances.accounts_balances.$.balances": balance}},
                )

            
            mongo_timer_end = datetime.datetime.now()
            logging.info(f"  DB insertion took: {mongo_timer_end-mongo_timer}")
        else:
            logging.warning("Got to new link balance db insertion but no transactions were found for the date range")  
        #####################################################################

##################################################################################################################################################################################################

'''
    Ran every 30 minutes
'''
def new_user_sync():
    logging.info("\nRuninng new user sync:\n")
    cm = Connection_Manager()
    
    # Open connection to postgres db
    conn = cm.get_postgres_connection()
    
    rows = conn.execute(
        #"SELECT DISTINCT id FROM public.users WHERE created_at >= (NOW() - INTERVAL '30 minutes')"
        "SELECT DISTINCT id FROM public.users" 
    ).fetchall()
    
    user_ids = [str(row[0]) for row in rows]

    # Get all user access tokens and upload transaction/balance them using the pave agent
    for user_id in tqdm(user_ids):
        rows = conn.execute(
            f"SELECT DISTINCT access_token FROM public.plaid_links WHERE user_id = '{user_id}'"
        ).fetchall()
        
        access_tokens = [decrypt(str(row[0])) for row in rows]
        time_in_days = 365 * 2

        for access_token in access_tokens:
            res = requests.post(
                f"http://127.0.0.1:8123/v1/users/{user_id}/upload?num_transaction_days={time_in_days}",
                json={"access_token": f"{access_token}"},
            )
            res = res.json()
            logging.debug(f"Got response from pave-agent: {res}")
            
            # Give pave agent some time to process transactions
            time.sleep(2)

        # Date ranges for pave
        start_date_str = (
            datetime.datetime.now() - datetime.timedelta(days=time_in_days)
        ).strftime("%Y-%m-%d")
        end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d")
        params = {"start_date": start_date_str, "end_date": end_date_str}
    
        # Get all transactions and upload them to mongodb
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
        insert_response_into_db(
            user_id=user_id,
            res=response,
            mongo_db=mongo_db,
            collection_name="transactions",
            response_column_name="transactions",
        )
        transaction_date_str=None
        if response.status_code == 200:
            transactions = response.json()["transactions"]
            transaction_date_str = transactions[len(transactions)-1]["date"]
            logging.info(f" > Earliest transaction: {transaction_date_str}")
            params["start_date"] = transaction_date_str
        #####################################################################
        
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
        
        
##################################################################################################################################################################################################
       
'''
    Hourly sync to update transactions created in the last hour
'''
def hourly_sync():
    logging.info("\nRuninng Hourly Sync:\n")
    
    rows = conn.execute(
        "SELECT * FROM public.plaid_transactions WHERE plaid_transactions.date >= (NOW() - INTERVAL '70 minutes')"
    ).fetchall()

    for row in tqdm(rows):
        row:dict = row._asdict()
        transaction = {
            "transaction_id": str(row["plaid_transaction_id"]),
            "account_id": str(row["plaid_account_id"]),
            "amount": float(row["amount"]),
            "date": str(row["authorized_date"]),
            "memo": " ".join(
                row["personal_finance_category"].values()
            )
            if row["personal_finance_category"]
            else "",
            "name": row["name"] if row["name"] else " ",
            "pending": row["pending"],
            "category": row["category"],
            "category_id": row["category_id"],
            "iso_currency_code": row["iso_currency_code"],
            "merchant_name": row["merchant_name"],
            "payment_channel": row["payment_channel"],
            "transaction_type": row["transaction_type"],
            "payment_meta": row["payment_meta"],
            "location": row["location"]
        }
        
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
            payload={"transactions": [transaction]},
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
            
            mongo_timer = datetime.datetime.now()
            mongo_collection = mongo_db["transactions"]
            transactions = response.json()["transactions"]

            if len(transactions) > 0:
                logging.info(f"Inserting {transactions} into transactions")
               
                mongo_collection.update_one(
                    {"user_id": str(user_id)},
                    {
                        "$addToSet": {"transactions.transactions": {"$each": transactions}},
                        "$set": {"transactions.to": end_date_str, "date": datetime.datetime.now()}
                    }
                )
                
                mongo_timer_end = datetime.datetime.now()
                logging.info(f"  DB insertion took: {mongo_timer_end-mongo_timer}")
            else:
                logging.warning("Got to hourly db insertion but no transactions were found for the date range")
        else:
            logging.error("Could not upload transaction to mongodb") 
        #####################################################################

##################################################################################################################################################################################################

'''
    Daily sync to update user balances for all users in the db for yesterday
'''
def daily_sync():
    logging.info("\nRuninng Daily Balance Sync:\n")
    
    rows = conn.execute(
        "SELECT DISTINCT id FROM public.users"
    ).fetchall()
    
    user_ids = [str(row[0]) for row in rows]

    # Get all user access tokens and upload transaction/balance them using the pave agent
    for user_id in tqdm(user_ids):
        rows = conn.execute(
            f"SELECT DISTINCT id FROM public.plaid_links WHERE user_id = '{user_id}'"
        ).fetchall()
        plaid_link_ids = [str(row[0]) for row in rows] 
    
        if len(plaid_link_ids) == 0:
            logging.warning(f"No plaid links for user {user_id}")
            continue
    
        rows = conn.execute(
            f"SELECT * FROM public.plaid_accounts WHERE plaid_accounts.link_id IN {str(tuple(plaid_link_ids)).replace(',)', ')')}"
        ).fetchall()
        
        accounts = [{
            "account_id": str(row["plaid_id"]),
            "balances": {
                "available": decrypt(row["balances_available"]),
                "current": decrypt(row["balances_current"]), 
                "iso_currency": decrypt(row["balances_iso_currency_code"]),
                "limit": decrypt(row["balances_limit"]),
                "unofficial_currency_code": decrypt(row["balances_unofficial_currency_code"]) 
            },
            "mask": decrypt(row["mask"]),
            "name": decrypt(row["name"]),
            "official_name": decrypt(row["official_name"]),
            "type": row["type"],
            "subtype": row["subtype"]
        }
        for row in rows
        ]
        
        response = handle_pave_request(
            user_id=user_id,
            method="post",
            endpoint=f"{pave_base_url}/{user_id}/balances",
            payload={"run_timestamp": str(datetime.datetime.now()), "accounts": accounts},
            headers=pave_headers,
            params=None,
        ) 
        #####################################################################

        if response.status_code == 200:
            mongo_db = cm.get_pymongo_table(pave_table)
            
            # Date ranges for pave
            start_date_str = (
                datetime.datetime.now() - datetime.timedelta(days=1)
            ).strftime("%Y-%m-%d")
            end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d")
            params = {"start_date": start_date_str, "end_date": end_date_str}
            
            # Store the transaction data from pave
            response = handle_pave_request(
                user_id=user_id,
                method="get",
                endpoint=f"{pave_base_url}/{user_id}/balances",
                payload=None,
                headers=pave_headers,
                params=params,
            )
            
            mongo_timer = datetime.datetime.now()
            mongo_collection = mongo_db["balances"]
            balances = response.json()["accounts_balances"]


            if len(balances) > 0:
                logging.info(f"Inserting {balances} into balances")

                mongo_collection.update_one(
                    {"user_id": str(user_id)},
                    {"$set": {"balances.to": end_date_str, "date": datetime.datetime.now()}}
                )
                for balance in balances:
                    mongo_collection.update_one(
                        {"user_id": str(user_id), "balances.accounts_balances": {"$elemMatch": {"account_id": balance["account_id"]}}},
                        {"$addToSet": {"balances.accounts_balances.$.balances": balance}}
                    )

                
                mongo_timer_end = datetime.datetime.now()
                logging.info(f"  DB insertion took: {mongo_timer_end-mongo_timer}")
            else:
                logging.warning("Got to daily db insertion but no transactions were found for the date range")
        else:
            logging.error("Could not upload balances to mongodb") 
        ##################################################################### 

##################################################################################################################################################################################################

'''
    Weekly/Daily 2 sync to update unified insight data
'''
def weekly_sync():
    logging.info("\nRuninng weekly sync:\n")
    cm = Connection_Manager()
    
    # Open connection to postgres db
    conn = cm.get_postgres_connection()
    
    rows = conn.execute(
        "SELECT DISTINCT id FROM public.users" 
    ).fetchall()
    
    user_ids = [str(row[0]) for row in rows]
    
    # Date ranges for pave, set for 2 years subject to change
    start_date_str = (
        datetime.datetime.now() - datetime.timedelta(days=365*2)
    ).strftime("%Y-%m-%d")
    end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d")
    params = {"start_date": start_date_str, "end_date": end_date_str}

    # Get all users unified insight data 
    for user_id in tqdm(user_ids):
        # Get all transactions and upload them to mongodb
        mongo_db = cm.get_pymongo_table(pave_table)

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

        # This creates a pretty big slowdown and we're not using it so I'll keep it
        # commented for now
        # # Store the attribute data from pave
        # params = {"date": end_date_str}
        # response = handle_pave_request(
        #     user_id=user_id,
        #     method="get",
        #     endpoint=f"{pave_base_url}/{user_id}/attributes",
        #     payload=None,
        #     headers=pave_headers,
        #     params=params,
        # )
        # insert_response_into_db(
        #     user_id=user_id,
        #     res=response,
        #     mongo_db=mongo_db,
        #     collection_name="attributes",
        #     response_column_name="attributes",
        # )
        # #####################################################################

##################################################################################################################################################################################################

# Open connection to postgres db
cm = Connection_Manager()
conn = cm.get_postgres_connection() 

if __name__ == "__main__":
    process_start = datetime.datetime.now()
    try:
        which = sys.argv[1]
        
        if which == "new":
            new_user_sync()
        elif which == "new2":
            new_link_sync()
        elif which == "hourly":
            hourly_sync()
        elif which == "daily":
            daily_sync() 
        elif which == "weekly":
            weekly_sync()
        else:
            raise Exception("You must provide either new_users, hourly, or daily")
    except Exception as e:
        print(e)
        logging.exception(e)
    
    cm.close_pymongo_connection()
    cm.close_postgres_connection(conn)
    process_end = datetime.datetime.now()
    logging.info(f"\nTotal runtime: {process_end-process_start}")
