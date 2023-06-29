from utils import *
import concurrent.futures

handler = RotatingFileHandler('/home/langston/pave-prism/logs/daily-balance-data-sync.log', 'a+', (1000**2)*200, 2)
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

process_start = datetime.datetime.now()

log_this("\n\nRuninng Balance Sync:\n", "info")
log_this(f"Process start: {process_start}", "info")

conn = get_backend_connection()
mongo_db = get_pymongo_connection()[pave_table]

rows = conn.execute(
    "SELECT DISTINCT id FROM public.users"
).fetchall()

user_ids = [str(row[0]) for row in rows]

def run_on_user(user_id):
    log_this(f"\n\nRunning for {user_id=}")
    rows = conn.execute(
        f"SELECT DISTINCT id FROM public.plaid_links WHERE user_id = '{user_id}'"
    ).fetchall()
    plaid_link_ids = [str(row[0]) for row in rows]

    if len(plaid_link_ids) == 0:
        log_this(f"\tNo plaid links for {user_id=}", "warning")
        return

    rows = conn.execute(
        f"SELECT data FROM public.plaid_raw_transaction_sets WHERE link_id IN {str(tuple(plaid_link_ids)).replace(',)', ')')} ORDER BY end_date DESC LIMIT 1"
    ).fetchall()

    accounts = []
    for row in rows:
        row = row._asdict()['data']
        for item in row:
            _accounts = item["accounts"]
            for account in _accounts:
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
                    for balance_obj in balances:
                        log_this(f"\tInserting {len(balance_obj['balances'])} balances", "info")
                        # Add each balance object that isn't already listed in the account @ balance_obj["account_id"]
                        mongo_collection.update_one(
                            {"user_id": str(user_id), "balances.accounts_balances": {"$elemMatch": {"account_id": balance_obj["account_id"]}}},
                            {"$addToSet": {"balances.accounts_balances.$.balances": {"$each": balance_obj["balances"]}},
                             "$set":{"balances.accounts_balances.$.days_negative": balance_obj["days_negative"],
                                "balances.accounts_balances.$.days_single_digit": balance_obj["days_single_digit"],
                                "balances.accounts_balances.$.days_double_digit": balance_obj["days_double_digit"],
                                "balances.accounts_balances.$.median_balance": balance_obj["median_balance"]}}
                        )

                    # Update the end date to today
                    mongo_collection.update_one(
                        {"user_id": str(user_id)},
                        {"$set": {"balances.to": end_date_str, "date": datetime.datetime.now()}},
                        bypass_document_validation = True
                    )
                except Exception as e:
                    log_this(f"COULD NOT UPDATE BALANCE FOR USER {user_id} ON DAILY SYNC", "error")
                    log_this(f"{e}", "error")
                    exit(1)

            else:
                log_this("\tGot to daily db insertion but no balances were found for the date range", "warning")
        except Exception as e:
            log_this("\tCould not find user after uploading balances", "error")
            log_this(f"{e}", "error")

        mongo_timer_end = datetime.datetime.now()
        log_this(f"\tDB insertion took: {mongo_timer_end-mongo_timer}", "info")

    else:
        log_this("\tCould not upload balances to mongodb", "error")


executor = concurrent.futures.ProcessPoolExecutor(10)
futures = [executor.submit(run_on_user, user_id) for user_id in user_ids]
concurrent.futures.wait(futures)

close_backend_connection()
close_pymongo_connection()
