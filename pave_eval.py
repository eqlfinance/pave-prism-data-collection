import base64
import datetime
import dotenv
import os
import pathlib
import requests
import json
from json import JSONDecodeError
import pymongo
import sqlalchemy
from google.cloud.sql.connector import Connector
from google.oauth2 import service_account
from google.cloud import secretmanager
from cryptography.fernet import Fernet, MultiFernet
from tqdm import tqdm
import logging
import time
from db_connections import Connection_Manager

logging.basicConfig(
    filename="pave-eval-"
    + datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    + ".log",
    format="%(name)s @ %(asctime)s: %(message)s",
    datefmt="%I:%M:%S",
    level=logging.DEBUG,
)


secret_manager_client = secretmanager.SecretManagerServiceClient()

pave_str = secret_manager_client.access_secret_version(
    name=f"projects/eql-data-processing/secrets/pave-prism-info/versions/latest"
).payload.data.decode("UTF-8")

pave_data = json.loads(pave_str)
pave_base_url = pave_data["PAVE_HOST"]
pave_x_api_key = pave_data["PAVE_X_API_KEY"]
pave_headers = {"Content-Type": "application/plaid+json", "x-api-key": pave_x_api_key}


cm = Connection_Manager()
mongo_db = cm.get_pymongo_table("pave")


def aggregate2(
    user_id: str,
    conn: sqlalchemy.engine.Connection,
    start_date_str: str = (
        datetime.datetime.now() - datetime.timedelta(days=180)
    ).strftime("%Y-%m-%d"),
    end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d"),
):

    account_data = {
        "run_timestamp": datetime.datetime.now().isoformat(),
        "accounts": [],
    }
    transaction_data = {
        "transactions": [],
        "start_date": start_date_str,
        "end_date": end_date_str,
    }

    link_select_string = (
        f"SELECT id FROM public.plaid_links WHERE user_id = '{user_id}'"
    )
    link_id_rows = conn.execute(link_select_string).fetchall()

    sql_account_string = f"SELECT data FROM public.plaid_raw_transaction_sets WHERE start_date >= '{start_date_str}'::date AND end_date <= '{end_date_str} 23:59:59'::date "

    if len(link_id_rows) > 1:
        link_ids = tuple([str(x[0]) for x in link_id_rows])
        sql_account_string += f"AND link_id IN {link_ids}"
    elif len(link_id_rows) == 1:
        sql_account_string += f"AND link_id = '{(link_id_rows[0])[0]}'"
    else:
        logging.warning(f"\tNo links found for user {user_id}")
        return {"accounts": account_data, "transactions": transaction_data}

    sql_account_string += " ORDER BY created_at DESC"

    raw_transactions = conn.execute(sql_account_string).fetchall()
    trans_ids = []
    account_ids = []

    for row in raw_transactions:
        for item in row[0]:
            #logging.debug(f"\t accounts :{item['accounts']}")
            #logging.debug(f"\t num transactions:{len(item['transactions'])}\n")
            #account_data["accounts"].extend(item["accounts"])
            #transaction_data["transactions"].extend(item["transactions"])

            for account in item["accounts"]:
                if account['account_id'] not in account_ids:
                    account_ids.append(account["account_id"])
                    account_data["accounts"].append(account)

            for transaction in item["transactions"]:
                if transaction["transaction_id"] not in trans_ids:
                    trans_ids.append(transaction["transaction_id"])

                    # print(transaction, "\n")

                    transaction_data["transactions"].append(
                        {
                            "transaction_id": transaction["transaction_id"],
                            "account_id": transaction["account_id"],
                            "amount": abs(float(transaction["amount"])),
                            "credit_or_debit": "CREDIT"
                            if float(transaction["amount"]) < 0
                            else "DEBIT",
                            "posted_date": transaction["date"],
                            "memo": " ".join(transaction["category"])
                            if transaction["category"]
                            else ""
                            + " ".join(
                                transaction["personal_finance_category"].values()
                            )
                            if transaction["personal_finance_category"]
                            else "",
                            "iso_currency_code": transaction["iso_currency_code"],
                            "authorized_date": transaction["authorized_date"],
                            "merchant_name": transaction["merchant_name"],
                            "payment_channel": transaction["payment_channel"],
                            "transaction_type": transaction["transaction_type"],
                        }
                    )

    return {"accounts": account_data, "transactions": transaction_data}



def pave_request(user_id, method, endpoint, payload, params, collection_name, response_column_name):
    logging.info("user_id:{}\n\tRequest: /{}, inserting response into: {}".format(user_id, endpoint, response_column_name))
    start = time.time()

    if method == "get":
        res = requests.get(f"{pave_base_url}/{user_id}/{endpoint}", json=payload, headers=pave_headers, params=params)
    elif method == "post":
        res = requests.post(f"{pave_base_url}/{user_id}/{endpoint}", json=payload, headers=pave_headers, params=params)
    else:
        raise ValueError("Method not understood {}".format(method))

    end = time.time()
    remaining = start + 1.5 - end
    if remaining > 0:
        time.sleep(remaining)

    res_code = res.status_code
    logging.info(f"\tResponse code: {res_code}, {res.json()=}")
    mongo_collection = mongo_db[collection_name]

    if res_code == 200:
        mongo_collection.replace_one(
            {"user_id": user_id},
            {
                response_column_name: res.json(),
                "user_id": user_id,
                "response_code": res.status_code,
                "date": datetime.datetime.now()
            },
            upsert=True
        )
    else:
        mongo_collection.replace_one(
            {"user_id": user_id},
            {
                "error": res.json(),
                "user_id": user_id,
                "response_code": res.status_code,
                "date": datetime.datetime.now()
            },
            upsert=True
        )
        logging.info("\tError on {} for user {}\n".format(endpoint, user_id))


"""
        BALANCES
"""


def post_pave_balance_upload(user_id: str, balances: dict):
    pave_request(user_id, "post", "balances", balances, None, "balance_uploads", "response")


def get_pave_balances(
    user_id: str,
    start_date_str: str = (
        datetime.datetime.now() - datetime.timedelta(days=180)
    ).strftime("%Y-%m-%d"),
    end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d"),
):

    params = {"start_date": start_date_str, "end_date": end_date_str}
    pave_request(user_id, "get", "balances", None, params, "balances", "balances")


"""
        TRANSACTIONS
"""


def post_pave_transaction_upload(user_id: str, transactions_dict: dict):
    logging.debug(f"\tPOST transaction_upload")
    endpoint = f"{pave_base_url}/{user_id}/transactions"

    trans = {"transactions": transactions_dict["transactions"]}

    params = {
        "resolve_duplicates": True,
        "start_date": transactions_dict["start_date"],
        "end_date": transactions_dict["end_date"],
    }

    logging.info(f"{len(trans['transactions'])=}")
    #res = requests.post(endpoint, json=trans, headers=pave_headers, params=params)
    pave_request(user_id, "post", "transactions", trans, params, "transaction_uploads", "response")


def get_pave_transactions(
    user_id: str,
    start_date_str: str = (
        datetime.datetime.now() - datetime.timedelta(days=180)
    ).strftime("%Y-%m-%d"),
    end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d"),
):

    logging.debug(f"\tGET pave_transactions")
    params = {"start_date": start_date_str, "end_date": end_date_str}
    endpoint = f"{pave_base_url}/{user_id}/transactions"

    #res = requests.get(endpoint, params=params, headers=pave_headers)
    pave_request(user_id, "get", "transactions", None, params, "transactions", "transactions")


"""
        Unified Insights
"""


def get_unified_insights(
    user_id: str,
    start_date_str: str = (
        datetime.datetime.now() - datetime.timedelta(days=180)
    ).strftime("%Y-%m-%d"),
    end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d"),
):

    logging.debug(f"\tGET unified_insights")
    endpoint = f"{pave_base_url}/{user_id}/unified_insights"
    params = {
        "start_date": start_date_str,
        "end_date": end_date_str,
        "with_transactions": False,
    }

    #res = requests.get(endpoint, headers=pave_headers, params=params)
    pave_request(user_id, "get", "unified_insights", None, params, "unified_insights", "unified_insights")

"""
        Financial Accounts
"""


def get_financial_accounts(user_id: str):
    logging.debug(f"\tGET financial_accounts")
    endpoint = f"{pave_base_url}/{user_id}/financial_accounts"

    #res = requests.get(endpoint, headers=pave_headers)


"""
        Attributes
"""


def get_attributes(
    user_id: str, date_str: str = datetime.datetime.now().strftime("%Y-%m-%d")
):
    logging.debug(f"\tGET attributes")
    endpoint = f"{pave_base_url}/{user_id}/attributes"

    params = {
        "date": date_str,
    }

    #res = requests.get(endpoint, headers=pave_headers, params=params)
    pave_request(user_id, "get", "attributes", None, params, "attributes", "attributes")


if __name__ == "__main__":
    start = datetime.datetime.now()

    env_user_ids = "aa319d2a-70b5-4019-83a9-733ed5bd0dbc,83dcc8c0-d6ed-4015-97dd-7b6022993f03,aeb83128-fe97-4178-9679-83ac0721195f,2efb959d-52a6-4229-8d82-8978a2cafcd5,27066e18-464a-461c-9d82-5f3c69ef0ac2"
    user_ids = []
    conn = cm.get_postgres_connection()

    if env_user_ids != "":
        user_ids = env_user_ids.split(",")
        logging.debug(f"Running eval for {len(user_ids)} user(s)\n")
    else:
        # Calculate for all users
        rows = conn.execute("SELECT DISTINCT id FROM public.users").fetchall()
        user_ids = [str(row[0]) for row in rows]
        logging.debug("Running evals for all users...\n")


    for user_id in tqdm(user_ids):
        logging.debug("Eval for user {}".format(user_id))

#        last_record = mongo_db.unified_insights.find(
#            filter={"user_id": str(user_id), "response_code": 200}, limit=1
#        ).sort([("date", pymongo.DESCENDING)])
#        # If an eval has been done in the last day skip for now
#        last_record = list(last_record)
#        if len(last_record) > 0 and last_record[0]["date"] > (
#            datetime.datetime.now() - datetime.timedelta(days=1)
#        ):
#            logging.warning(f"\tLast record for {user_id} too recent, skipping...")
#            continue

        user_data = aggregate2(user_id, conn)
        logging.info(f"\tAggregated data: {len(user_data['accounts']['accounts'])=}, {len(user_data['transactions']['transactions'])=}")
        b_data = user_data["accounts"]
        t_data = user_data["transactions"]

        if len(t_data) >= 1000:
            logging.info("\tCut transactions down to 1000")
            t_data = t_data[:1000]

        if len(b_data) == 0 or len(t_data["transactions"]) == 0:
            logging.warning(f"\tEmpty data for user {user_id}, skipping eval...")
            continue


        post_pave_balance_upload(user_id, b_data)
        post_pave_transaction_upload(user_id, t_data)
        get_pave_balances(user_id)
        get_pave_transactions(user_id)
        #get_unified_insights(user_id)
        #get_financial_accounts(user_id)
        #get_attributes(user_id)

    cm.close_postgres_connection(conn)
    end = datetime.datetime.now()
    logging.info(f"\nTotal runtime: {end-start}")
