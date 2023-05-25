import datetime
import subprocess
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

# Get pave secret values
secret_manager_client = secretmanager.SecretManagerServiceClient()

pave_table = "pave"

# Decrpytion keys
keys = secret_manager_client.access_secret_version(
    name=f"projects/eql-data-processing/secrets/pave-agent-decryption-keys/versions/latest"
).payload.data.decode("UTF-8")
keys = json.loads(keys)["KEYS"]

# Pave url necessities
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

def log_this(message:str, severity:str = "debug"):
    logger.log(logging._nameToLevel[severity.upper()], message)
    subprocess.run(["gcloud", "logging", "write", "stevenslav", message, f"--severity={severity.upper()}", "--quiet", "--verbosity=none", "--no-user-output-enabled"], stdout=subprocess.PIPE)

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

    log_this(f"Making pave request to {endpoint}", "info")
    request_timer = datetime.datetime.now()

    if method == "get":
        res = requests.get(f"{endpoint}", json=payload, headers=headers, params=params)
    elif method == "post":
        res = requests.post(f"{endpoint}", json=payload, headers=headers, params=params)
    else:
        raise ValueError("Method not understood {}".format(method))

    res_code = res.status_code
    res_json = json.dumps(res.json())
    log_this(f"\tResponse: {res_code} -> {res_json[:100]} ... {res_json[-100:]}", "info")

    if res_code == 429:
        sleep = 1 if last_wait == 0 else last_wait * 2
        log_this(f"\tRequest limit reached, waiting {sleep} second(s)", "error")
        time.sleep(sleep)
        return handle_pave_request(
            user_id, method, endpoint, payload, headers, params, sleep
        )
    else:
        request_timer_end = datetime.datetime.now()
        log_this(f"Pave request to {endpoint} took: {request_timer_end-request_timer}\n", "info")

        return res

def insert_response_into_db(
    user_id: str, res, mongo_db, collection_name: str, response_column_name: str
):
    log_this("Inserting response into: {}.{}".format(collection_name, response_column_name), "info")
    mongo_timer = datetime.datetime.now()
    mongo_collection = mongo_db[collection_name]
    res_code = res.status_code

    if res_code == 200:
        try:
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
        except Exception as e:
            log_this(f"COULD NOT INSERT response into {response_column_name} FOR USER {user_id}", "error")
            log_this(f"{e}", "error")
    else:
        log_this("\tCan't insert to {}: {} {}\n".format(collection_name, res_code, res.json()), "warning")

    mongo_timer_end = datetime.datetime.now()
    log_this(f"DB insertion to {collection_name} took: {mongo_timer_end-mongo_timer}\n", "warning")


'''
    Ran every 30 minutes
'''
def new_link_sync():
    log_this("Runinng new link sync:\n", "info")
    cm = Connection_Manager()

    # Open connection to postgres db
    conn = cm.get_postgres_connection()

    rows = conn.execute(
        "SELECT DISTINCT access_token, user_id FROM public.plaid_links WHERE created_at >= (NOW() - INTERVAL '35 minutes')"
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
        log_this(f"\tGot response from pave-agent: {res}", "debug")

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

        if response.status_code != 200:
            log_this("Non 200 return code on transactions", "exception")
            log_this(f"{response.json()}", "exception")
        else:
            mongo_timer = datetime.datetime.now()
            mongo_collection = mongo_db["transactions"]
            transactions = response.json()["transactions"]

            if len(transactions) > 0:
                log_this(f"\tInserting {json.dumps(transactions)[:200]}... into transactions", "info")

                try:
                    mongo_collection.update_one(
                        {"user_id": str(user_id)},
                        {
                            "$addToSet": {"transactions.transactions": {"$each": transactions}},
                            "$set": {"transactions.to": end_date_str, "date": datetime.datetime.now()}
                        }
                    )
                except Exception as e:
                    log_this(f"COULD NOT UPDATE TRANSACTIONS FOR USER {user_id} ON LINK SYNC", "error")
                    log_this(f"{e}", "error")

                mongo_timer_end = datetime.datetime.now()
                log_this(f"\tDB insertion took: {mongo_timer_end-mongo_timer}", "info")

                transaction_date_str = transactions[len(transactions)-1]["date"]
                params["start_date"] = transaction_date_str
            else:
                log_this("\tGot to new link transaction db insertion but no transactions were found for the date range", "warning")

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

        if response.status_code != 200:
            log_this("Non 200 return code on transactions", "exception")
            log_this(f"{response.json()}", "exception")
        else:
            mongo_timer = datetime.datetime.now()
            mongo_collection = mongo_db["balances"]
            balances = response.json()["accounts_balances"]


            if len(balances) > 0:
                log_this(f"\tInserting {json.dumps(balances)[:200]} into balances", "info")

                try:
                    mongo_collection.update_one(
                        {"user_id": str(user_id)},
                        {"$set": {"balances.accounts_balances": balances}}
                    )

                    mongo_collection.update_one(
                        {"user_id": str(user_id)},
                        {"$set": {"balances.to": end_date_str, "date": datetime.datetime.now()}},
                        bypass_document_validation = True
                    )
                except Exception as e:
                    log_this(f"COULD NOT UPDATE BALANCES FOR USER {user_id} ON LINK SYNC", "error")
                    log_this(f"{e}")

                mongo_timer_end = datetime.datetime.now()
                log_this(f"\tDB insertion took: {mongo_timer_end-mongo_timer}", "info")
            else:
                log_this(f"\tDB insertion took: {mongo_timer_end-mongo_timer}", "warning")
            #####################################################################

##################################################################################################################################################################################################

'''
    Ran every 30 minutes
'''
def new_user_sync():
    log_this("Runinng new user sync:\n", "info")
    cm = Connection_Manager()

    # Open connection to postgres db
    conn = cm.get_postgres_connection()

    rows = conn.execute(
        "SELECT DISTINCT id FROM public.users WHERE created_at >= (NOW() - INTERVAL '35 minutes')"
        #"SELECT DISTINCT id FROM public.users"
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
            log_this(f"\tGot response from pave-agent: {res}", "debug")

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
        if response.status_code == 200:
            transactions = response.json()["transactions"]
            if len(transactions) > 0:
                transaction_date_str = transactions[len(transactions)-1]["date"]
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
                log_this("\tInserting response into: {}".format(title), "info")
                mongo_collection = mongo_db[title]

                try:
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
                except Exception as e:
                    log_this(f"COULD NOT INSERT {title} FOR USER {user_id} ON NEW USER SYNC", "error")
                    log_this(f"{e}", "error")

        else:
            log_this("\tCan't insert: {} {}\n".format(response.status_code, response.json()), "warning")
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
    log_this("Runinng Hourly Sync:\n", "info")

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
                log_this(f"\tInserting {json.dumps(transactions)[:100]} into transactions", "info")

                mongo_collection.update_one(
                    {"user_id": str(user_id)},
                    {
                        "$addToSet": {"transactions.transactions": {"$each": transactions}},
                        "$set": {"transactions.to": end_date_str, "date": datetime.datetime.now()}
                    }
                )

                mongo_timer_end = datetime.datetime.now()
                log_this(f"\tDB insertion took: {mongo_timer_end-mongo_timer}", "info")
            else:
                log_this("\tGot to hourly db insertion but no transactions were found for the date range", "warning")
        else:
            log_this("Could not upload transactions to mongodb", "error")
        #####################################################################

##################################################################################################################################################################################################

'''
    Daily sync to update user balances for all users in the db for yesterday
'''
def daily_sync():
    log_this("Runinng Daily Balance Sync:\n", "error")

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
            log_this(f"\tNo plaid links for user {user_id}", "warning")
            continue

        rows = conn.execute(
            f"SELECT data FROM public.plaid_raw_transaction_sets WHERE link_id IN {str(tuple(plaid_link_ids)).replace(',)', ')')} ORDER BY end_date DESC LIMIT 1"
        ).fetchall()

        accounts = []
        for row in rows:
            row = row._asdict()['data']
            for item in row:
                _accounts = item["accounts"]
                for account in _accounts:

                    log_this(f"Hello: {account}", "error")
                    if account["account_id"] not in [x["account_id"] for x in accounts]:
                        accounts.append({
                            "account_id": str(account["account_id"]),
                            "balances": {
                                "available": account["balances"]["available"],
                                "current": account["balances"]["current"],
                                "iso_currency_code": account["balances"]["iso_currency_code"],
                                "limit": account["balances"]["limit"],
                                "unofficial_currency_code": account["balances"]["unofficial_currency_code"]
                            },
                            "mask": account["mask"],
                            "name": account["name"],
                            "official_name": account["official_name"],
                            "type": account["type"],
                            "subtype": account["subtype"]
                        })

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
            try:
                mongo_collection = mongo_db["balances"]
                balances = response.json()["accounts_balances"]

                if len(balances) > 0:

                    try:
                        for balance in balances:
                            log_this(f"\tInserting {json.dumps(balance['balances'])[:100]} into balances", "info")
                            mongo_collection.update_one(
                                {"user_id": str(user_id), "balances.accounts_balances": {"$elemMatch": {"account_id": balance["account_id"]}}},
                                {"$addToSet": {"balances.accounts_balances.$.balances": {"$each": balance["balances"]}}}
                            )

                        mongo_collection.update_one(
                            {"user_id": str(user_id)},
                            {"$set": {"balances.to": end_date_str, "date": datetime.datetime.now()}},
                            bypass_document_validation = True
                        )
                    except Exception as e:
                        log_this(f"COULD NOT UPDATE BALANCE FOR USER {user_id} ON DAILY SYNC", "error")
                        log_this(f"{e}", "error")

                else:
                    log_this("\tGot to daily db insertion but no transactions were found for the date range", "warning")
            except Exception as e:
                log_this("\tCould not find user after uploading balances", "error")
                log_this(f"{e}", "error")

            mongo_timer_end = datetime.datetime.now()
            log_this(f"\tDB insertion took: {mongo_timer_end-mongo_timer}", "info")

        else:
            log_this("\tCould not upload balances to mongodb", "error")
        #####################################################################

##################################################################################################################################################################################################

'''
    Weekly/Daily 2 sync to update unified insight data
'''
def weekly_sync():
    log_this("Runinng weekly sync:\n", "info")
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
                log_this("\tInserting response into: {}".format(title), "info")
                mongo_collection = mongo_db[title]

                try:
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
                except Exception as e:
                    log_this(f"COULD NOT UPDATE {title} FOR USER {user_id} ON DAILY SYNC", "error")
                    log_this(f"{e}", "error")
        else:
            log_this("\tCan't insert: {} {}\n".format(response.status_code, response.json()), "warning")
        #####################################################################

        # We may actually want this data for decisioning so we can take the slowdown
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

# Open connection to postgres db
cm = Connection_Manager()
conn = cm.get_postgres_connection()

logger = logging.getLogger("stevenslav2")
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('[%(levelname)s] %(name)-10s @ %(asctime)s: %(message)s')

normal_log_handler = RotatingFileHandler('/home/langston/pave-prism/stevenslav2.log', 'a', 1000**3, 2)
normal_log_handler.setFormatter(formatter)
normal_log_handler.setLevel(logging.DEBUG)
logger.addHandler(normal_log_handler)

if __name__ == "__main__":
    process_start = datetime.datetime.now()
    try:
        if len(sys.argv) == 1:
            raise Exception("You must provide either user, link, hourly, daily, or weekly")

        which = sys.argv[1]

        if which == "user":
            handler = RotatingFileHandler('/home/langston/pave-prism/logs/new-user-data-sync.log', 'a', (1000**2)*200, 2)
            handler.setFormatter(formatter)
            handler.setLevel(logging.INFO)
            logger.addHandler(handler)

            log_this(f"Process start: {process_start}", "info")
            new_user_sync()
        elif which == "link":
            handler = RotatingFileHandler('/home/langston/pave-prism/logs/new-link-data-sync.log', 'a', (1000**2)*200, 2)
            handler.setFormatter(formatter)
            handler.setLevel(logging.INFO)
            logger.addHandler(handler)

            log_this(f"Process start: {process_start}", "info")
            new_link_sync()
        elif which == "hourly":
            handler = RotatingFileHandler('/home/langston/pave-prism/logs/hourly-transaction-data-sync.log', 'a', (1000**2)*200, 2)
            handler.setFormatter(formatter)
            handler.setLevel(logging.INFO)
            logger.addHandler(handler)

            log_this(f"Process start: {process_start}", "info")
            hourly_sync()
        elif which == "daily":
            handler = RotatingFileHandler('/home/langston/pave-prism/logs/daily-balance-data-sync.log', 'a', (1000**2)*200, 2)
            handler.setFormatter(formatter)
            handler.setLevel(logging.INFO)
            logger.addHandler(handler)

            log_this(f"Process start: {process_start}", "info")
            daily_sync()
        elif which == "weekly":
            handler = RotatingFileHandler('/home/langston/pave-prism/logs/weekly-recurring-data-sync.log', 'a', (1000**2)*200, 2)
            handler.setFormatter(formatter)
            handler.setLevel(logging.INFO)
            logger.addHandler(handler)

            log_this(f"Process start: {process_start}", "info")
            weekly_sync()
        else:
            raise Exception("You must provide either user, link, hourly, daily, or weekly")
    except Exception as e:
        logger.exception(e)

    cm.close_pymongo_connection()
    cm.close_postgres_connection(conn)
    process_end = datetime.datetime.now()
    log_this(f"Total runtime: {process_end-process_start}\n\n", "info")
