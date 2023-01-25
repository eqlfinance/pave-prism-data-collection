import base64
import json
import math
import os
import pathlib
from typing import Dict
import sqlalchemy
import dotenv
import requests
import datetime
from google.cloud.sql.connector import Connector
from google.cloud import secretmanager
import pymongo
from tqdm import tqdm
import logging


from db_connections import Connection_Manager

logging.basicConfig(
    filename="prism-eval-"
    + datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    + ".log",
    format="%(name)s @ %(asctime)s: %(message)s",
    datefmt="%I:%M:%S",
    level=logging.DEBUG,
)

secret_manager_client = secretmanager.SecretManagerServiceClient()

prism_str = secret_manager_client.access_secret_version(
    name=f"projects/eql-data-processing/secrets/pave-prism-info/versions/latest"
).payload.data.decode("UTF-8")

prism_data = json.loads(prism_str)
prism_host = prism_data["PRISM-HOST"]
prism_token = prism_data["PRISM_ACCESS_TOKEN"]

cm = Connection_Manager()

# print("Connecting mongodb...")
mongo_db = cm.get_pymongo_table("prism")
# print(f"Connected mongo client at db {mongo_db.name}")


def aggregate2(
    user_id: str,
    conn: sqlalchemy.engine.Connection,
    start_date_str: str = (
        datetime.datetime.now() - datetime.timedelta(days=180)
    ).strftime("%Y-%m-%d"),
    end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d"),
):

    # account_data = {"run_timestamp": datetime.datetime.now().isoformat(), "accounts": []}
    # transaction_data = {"transactions": [], "start_date": start_date_str, "end_date": end_date_str}

    cashflow_data = {"accounts": [], "transactions": []}

    link_select_string = (
        f"SELECT id FROM public.plaid_links WHERE user_id = '{user_id}'"
    )
    link_id_rows = conn.execute(link_select_string).fetchall()

    sql_account_string = f"SELECT created_at, data FROM public.plaid_raw_transaction_sets WHERE start_date >= '{start_date_str}'::date AND end_date <= '{end_date_str}'::date "

    if len(link_id_rows) > 1:
        link_ids = tuple([str(x[0]) for x in link_id_rows])
        sql_account_string += f"AND link_id IN {link_ids}"
    elif len(link_id_rows) == 1:
        sql_account_string += f"AND link_id = '{(link_id_rows[0])[0]}'"
    else:
        return cashflow_data

    raw_transactions = conn.execute(sql_account_string).fetchall()


    newest_account_dict = {}
    trans_ids = []
    for row in raw_transactions:
        # print(row)
        for item in row[1]:
            # print(f"{len(item['transactions'])=}, {row[0]}")
            for account in item["accounts"]:
                if (
                    account["account_id"] not in newest_account_dict.keys()
                    or account["account_id"] in newest_account_dict.keys()
                    and newest_account_dict[account["account_id"]]["date"] < row[0]
                ):

                    newest_account_dict[account["account_id"]] = {"date": row[0]}

                    a = {
                        "account_id": account["account_id"],
                        "account_type": account["subtype"],
                        "balance_date": row[0].strftime("%Y-%m-%d"),
                        "available_balance": account["balances"]["available"],
                        "current_balance": account["balances"]["current"],
                        "iso_currency_code": account["balances"]["iso_currency_code"],
                        "institution_id": item["item"]["institution_id"],
                    }
                    newest_account_dict[account["account_id"]]["account"] = a
                else:
                    pass

            for transaction in item["transactions"]:

                if transaction["transaction_id"] not in trans_ids:
                    trans_ids.append(transaction["transaction_id"])

                    # print(transaction, "\n")

                    cashflow_data["transactions"].append(
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

            for account_data in newest_account_dict.values():
                cashflow_data["accounts"].append(account_data["account"])

    return cashflow_data


def calculate_new_cashscore(user_id: str, conn: sqlalchemy.engine.Connection):
    responses = mongo_db["responses"]
    cashscore_data = responses.find_one(
        {"user_id": str(user_id), "status_code": 200}, sort=[("created_at", -1)]
    )

    thirty_days_ago = datetime.datetime.now() - datetime.timedelta(days=30)
    if cashscore_data is None or cashscore_data["created_at"].replace(
        tzinfo=datetime.timezone.utc
    ) < thirty_days_ago.replace(tzinfo=datetime.timezone.utc):
        logging.info("Calculating new cashscore for user: {}".format(user_id))
    else:
        logging.warning(
            "\tCashscore calcaluted recently for user {} -> Score:{} @ {}\n".format(
                user_id, 
                cashscore_data['response']['products']['cashscore'], 
                cashscore_data['created_at'].strftime('%Y-%m-%d')
            )
        )
        return

    cashflow_data = aggregate2(user_id, conn)
    # print(f"\t{len(cashflow_data['transactions'])=}")

    if len(cashflow_data["accounts"]) == 0:
        logging.info("\tNo accounts, skipping eval...")
        return
    if len(cashflow_data["transactions"]) == 0:
        logging.info("\tNo transactions, skipping eval...")
        return
    elif len(cashflow_data["transactions"]) < 25:
        logging.warning("\tNot enough transactions, skipping eval...")
        return

    date_min, date_max = min(
        cashflow_data["transactions"],
        key=lambda t: datetime.datetime.strptime(t["posted_date"], "%Y-%m-%d"),
    ), max(
        cashflow_data["transactions"],
        key=lambda t: datetime.datetime.strptime(t["posted_date"], "%Y-%m-%d"),
    )

    delta_time = datetime.datetime.fromisoformat(
        date_max["posted_date"]
    ) - datetime.datetime.fromisoformat(date_min["posted_date"])

    logging.info(f"\t{len(cashflow_data['transactions'])=}\n{delta_time=}")

#    if delta_time < datetime.timedelta(days=90):
#        logging.warning("\tLess than 90 days of transaction history")
#        return

    payload = {
        "customer_id": user_id,
        "cashflow_data": cashflow_data,
        "evaluation_date": datetime.datetime.now().strftime("%Y-%m-%d"),
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {prism_token}",
    }

    endpoint = (
        prism_host
        + "/v2/evaluation?cashscore=1&insights=1&categories=1&income=1"
    )
    res = requests.post(endpoint, data=json.dumps(payload), headers=headers)
    response = convert_nans(res.json())
    #logging.debug(f"\t{response=}")

    mongo_db.responses.replace_one(
        {
                "user_id": user_id
        },
        {
            "created_at": datetime.datetime.now(),
            "status_code": res.status_code,
            "response": response,
            "user_id": user_id,
        },
        upsert=True
    )


    try:
        cashscore = response["products"]["cashscore"]["result"]
        insights = response["products"]["insights"]["result"]
        categories = response["products"]["categories"]["result"]
        income = response["products"]["income"]["result"]

        mongo_db.cashscores.replace_one(
            {
                "user_id": user_id
            },
            {
                "created_at": datetime.datetime.now(),
                "cashscore": cashscore,
                "user_id": user_id,
            },
            upsert=True
        )

        mongo_db.insights.replace_one(
            {
                "user_id": user_id
            },
            {
                "created_at": datetime.datetime.now(),
                "insights": insights,
                "user_id": user_id,
            },
            upsert=True
        )

        mongo_db.categories.replace_one(
            {
                "user_id": user_id
            },
            {
                "created_at": datetime.datetime.now(),
                "categories": categories,
                "user_id": user_id,
            },
            upsert=True
        )

        mongo_db.incomes.replace_one(
            {
                "user_id": user_id
            },
            {
                "created_at": datetime.datetime.now(),
                "income": income,
                "user_id": user_id,
            },
            upsert=True
        )

        logging.debug("Inserted into tables!")
    except:
        logging.debug(f"No products recieved for user: {user_id}")


def convert_nans(obj: Dict):
    for key, value in obj.items():
        if type(value) == dict:
            obj[key] = convert_nans(value)
        elif type(value) not in [str, list] and math.isnan(value):
            obj[key] = ""

    return obj


if __name__ == "__main__":
    param_user_ids = ""

    conn = cm.get_postgres_connection()
    start = datetime.datetime.now()
    if param_user_ids != "":
        user_ids = param_user_ids.split(",")
        logging.debug(f"Running eval for {len(user_ids)} user(s)\n")
    else:
        # Calculate for all users
        rows = conn.execute("SELECT id FROM public.users").fetchall()
        user_ids = [str(u[0]) for u in rows]
        logging.debug("Running evals for all users...\n")

    for user_id in tqdm(user_ids):
        calculate_new_cashscore(user_id, conn)

    end = datetime.datetime.now()
    logging.info(f"\nTotal runtime: {end-start}")
    cm.close_postgres_connection(conn)