from utils import *

handler = RotatingFileHandler('/home/langston/pave-prism/logs/new-link-data-sync.log', 'a', (1000**2)*200, 2)
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

process_start = datetime.datetime.now()

log_this("\n\nRuninng new link sync:\n", "info")
log_this(f"Process start: {process_start}", "info")

# Open connection to postgres db
conn = get_backend_connection()
mongo_db = get_pymongo_connection()[pave_table]

rows = conn.execute(
    "SELECT DISTINCT access_token, user_id FROM public.plaid_links WHERE created_at >= (NOW() - INTERVAL '10 hours')"
).fetchall()

for row in tqdm(rows):
    row = row._asdict()
    access_token, user_id = decrypt(row["access_token"]), str(row["user_id"])
    time_in_days = 365 * 2

    log_this(f"{user_id=}")
    pave_agent_start = datetime.datetime.now()
    res = requests.post(
        f"http://127.0.0.1:8123/v1/users/{user_id}/upload?num_transaction_days={time_in_days}",
        json={"access_token": f"{access_token}"},
    )
    # Give pave agent some time to process transactions
    time.sleep(2)
    pave_agent_end = datetime.datetime.now()
    log_this(f"  Pave Agent res code: {res.status_code}, took {pave_agent_end-pave_agent_start}", "debug")

    # Date ranges for pave
    start_date_str = (
        datetime.datetime.now() - datetime.timedelta(days=time_in_days)
    ).strftime("%Y-%m-%d")
    end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d")
    params = {"start_date": start_date_str, "end_date": end_date_str}

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
        log_this("        Non 200 return code on transactions", "exception")
    else:
        mongo_timer = datetime.datetime.now()
        mongo_collection = mongo_db["transactions"]
        transactions = response.json()["transactions"]

        if len(transactions) > 0:
            log_this(f"   Inserting {json.dumps(transactions)[:200]}... into transactions", "info")

            try:
                mongo_collection.update_one(
                    {"user_id": str(user_id)},
                    {
                        "$addToSet": {"transactions.transactions": {"$each": transactions}},
                        "$set": {"transactions.to": end_date_str, "date": datetime.datetime.now()}
                    }
                )
            except Exception as e:
                log_this(f"        COULD NOT UPDATE TRANSACTIONS FOR USER {user_id} ON LINK SYNC", "error")
                log_this(f"        {e}", "error")

            mongo_timer_end = datetime.datetime.now()
            log_this(f"      Transaction insertion took: {mongo_timer_end-mongo_timer}", "info")

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
        log_this("    Non 200 return code on balances", "exception")
    else:
        mongo_timer = datetime.datetime.now()
        mongo_collection = mongo_db["balances"]
        balances = response.json()["accounts_balances"]

        if len(balances) > 0:
            log_this(f"    Inserting {json.dumps(balances)[:200]} into balances", "info")

            try:
                for balance_obj in balances:
                    # Add each balance object that isn't already listed in the account @ balance_obj["account_id"]
                    result = mongo_collection.update_one(
                        {"user_id": str(user_id), "balances.accounts_balances": {"$elemMatch": {"account_id": balance_obj["account_id"]}}},
                        {"$addToSet": {"balances.accounts_balances.$.balances": {"$each": balance_obj["balances"]}},
                         "$set":{"balances.accounts_balances.$.days_negative": balance_obj["days_negative"],
                                "balances.accounts_balances.$.days_single_digit": balance_obj["days_single_digit"],
                                "balances.accounts_balances.$.days_double_digit": balance_obj["days_double_digit"],
                                "balances.accounts_balances.$.median_balance": balance_obj["median_balance"]}}
                    )

                    if result.matched_count == 0:
                        log_this(f"\tInserting {json.dumps(balance_obj['balances'])} into balances", "info")
                        mongo_collection.update_one(
                            {"user_id": str(user_id)},
                            {"$push": {"balances.accounts_balances": balances},
                             "$set":{"balances.accounts_balances.$.days_negative": balance_obj["days_negative"],
                                "balances.accounts_balances.$.days_single_digit": balance_obj["days_single_digit"],
                                "balances.accounts_balances.$.days_double_digit": balance_obj["days_double_digit"],
                                "balances.accounts_balances.$.median_balance": balance_obj["median_balance"]}}
                        )

                mongo_collection.update_one(
                    {"user_id": str(user_id)},
                    {"$set": {"balances.to": end_date_str, "date": datetime.datetime.now()}},
                    bypass_document_validation = True
                )
            except Exception as e:
                log_this(f"    COULD NOT UPDATE BALANCES FOR USER {user_id} ON LINK SYNC", "error")
                log_this(f"    {e}", "error")

            mongo_timer_end = datetime.datetime.now()
            log_this(f"    Balance insertion took: {mongo_timer_end-mongo_timer}", "info")


close_backend_connection()
close_pymongo_connection()
