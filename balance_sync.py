from utils import *

handler = RotatingFileHandler('/home/langston/pave-prism/logs/daily-balance-data-sync.log', 'a+', (1000**2)*200, 2)
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

process_start = datetime.datetime.now()

log_this(f"\n\nRuninng Balance Sync Process start: {process_start}", "info")

conn = get_backend_connection()
mongo_db = get_pymongo_connection()[pave_table]

# balance_sync_user_set_divisor = os.getenv("BALANCE_SYNC_USD")
# if balance_sync_user_set_divisor is None:
#     balance_sync_user_set_divisor = 6
#     subprocess.call([f'export BALANCE_SYNC_USD={str(balance_sync_user_set_divisor)}'], shell=True)

# balance_sync_counter = os.getenv('BALANCE_SYNC_COUNTER')
# if balance_sync_counter is None:
#     balance_sync_counter = 0
# else:
#     balance_sync_counter = (balance_sync_counter+1) % balance_sync_user_set_divisor
# subprocess.call([f'export BALANCE_SYNC_COUNTER={str(balance_sync_counter)}'], shell=True)

rows = conn.execute(
    "SELECT DISTINCT id FROM public.users ORDER BY id ASC"
).fetchall()

# user_set_length = len(rows) // balance_sync_user_set_divisor
# user_set_start_idx = int((len(rows) * balance_sync_counter)/balance_sync_user_set_divisor)
# user_ids = [str(row[0]) for row in rows[user_set_start_idx : user_set_start_idx + user_set_length]]
# log_this(f"Running for {user_set_length} users [{user_set_start_idx} -> {user_set_start_idx + user_set_length}]")
user_ids = [str(row[0]) for row in rows]

def run_on_user(user_id):
    start = datetime.datetime.now()
    log_this(f"\nRunning Balance Sync for {user_id=} ({start})")

    # Select all user plaid links
    rows = conn.execute(
        f"SELECT DISTINCT id FROM public.plaid_links WHERE user_id = '{user_id}' AND status 'active'"
    ).fetchall()
    plaid_link_ids = [str(row[0]) for row in rows]

    if len(plaid_link_ids) == 0:
        log_this(f"\tNo plaid links for {user_id=}", "warning")
    else:
        rows = conn.execute(
            f"SELECT data FROM public.plaid_raw_transaction_sets WHERE link_id IN {str(tuple(plaid_link_ids)).replace(',)', ')')} ORDER BY end_date, created_at DESC LIMIT 1"
        ).fetchall()


        accounts = []
        for row in rows:
            row = row._asdict()['data']
            for item in row:
                _accounts = item["accounts"]
                for account in _accounts:
                    if account["account_id"] not in [x["account_id"] for x in accounts]:
                        log_this(f"\tProcessing account {account['account_id']}")
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

        log_this(f"\tGot balance_objects:\n{accounts=}")

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
            # Date ranges for pave, pull for 90 days
            num_balance_days = 90
            start_date_str = (
                datetime.datetime.now() - datetime.timedelta(days=num_balance_days)
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
                accounts_balances = response.json()["accounts_balances"]

                if len(accounts_balances) > 0:
                    try:
                        find_balance_doc_timer = datetime.datetime.now()
                        current_balance_document = mongo_collection.find_one({"user_id": user_id})
                        find_balance_doc_timer_end = datetime.datetime.now()
                        log_this(f"\tInserting balances for {len(accounts_balances)} accounts.\n\tPulling balances from mongodb took {find_balance_doc_timer_end-find_balance_doc_timer}", "info")

                        if not current_balance_document:
                            current_balance_document = []

                        for balance_obj in accounts_balances:
                            # Perform an aggregation to get the balances currently in the db
                            current_balances = [x for x in current_balance_document['balances']['accounts_balances'] if x['account_id'] == balance_obj["account_id"]]
                            current_balances = [*current_balances[:-(len(balance_obj["balances"])+1)], *balance_obj["balances"]]

                            matched = mongo_collection.update_one(
                                {"user_id": str(user_id), "balances.accounts_balances": {"$elemMatch": {"account_id": balance_obj["account_id"]}}},
                                {
                                    #"$addToSet": {"balances.accounts_balances.$.balances": {"$each": balance_obj["balances"]}},
                                    "$set":{
                                        "balances.accounts_balances.$.balances": current_balances,
                                        "balances.accounts_balances.$.days_negative": balance_obj["days_negative"],
                                        "balances.accounts_balances.$.days_single_digit": balance_obj["days_single_digit"],
                                        "balances.accounts_balances.$.days_double_digit": balance_obj["days_double_digit"],
                                        "balances.accounts_balances.$.median_balance": balance_obj["median_balance"]
                                    }
                                }
                            ).matched_count

                            if matched == 0:
                                balance_obj["balances"] = current_balances
                                mongo_collection.update_one(
                                    {"user_id": str(user_id)},
                                    {
                                        "$push":{
                                            "balances.accounts_balances": balance_obj,
                                        }
                                    }
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
            log_this("\tCould not upload balances to Pave", "error")

    end = datetime.datetime.now()
    print(f'{user_id} balance sync took: {end-start}')

with concurrent.futures.ProcessPoolExecutor(10) as executor:
    futures = [executor.submit(run_on_user, user_id) for user_id in user_ids]

    done, incomplete = concurrent.futures.wait(futures, timeout=60*30)
    log_this(f"Balance Sync: Ran on {len(done)}/{len(user_ids)} users ({len(incomplete)} incomplete)")

close_backend_connection()
close_pymongo_connection()

process_end = datetime.datetime.now()
log_this(f"Balance Sync: {process_start} -> {process_end} | Total run time: {process_end-process_start}", "info")
