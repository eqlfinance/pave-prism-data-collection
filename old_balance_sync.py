from utils import *

handler = RotatingFileHandler(f'{home_path}logs/daily-balance-data-sync.log', 'a+', (1000**2)*200, 2)
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

process_start = datetime.datetime.now()

log_this(f"Runinng Balance Sync Process start: {process_start}", "info")

conn = get_backend_connection()
mongo_db = get_pymongo_connection()[pave_table]

# This is the way we run only on a set of users so as the user set exapnds
# the speed doesn't become unreasonable
balance_sync_user_set_divisor = counters["balance_sync_usd"]
balance_sync_counter = counters["balance_sync_counter"]
balance_sync_counter = (balance_sync_counter+1) % balance_sync_user_set_divisor

with open(f'{home_path}counters.json', 'w') as file:
    counters["balance_sync_counter"] = balance_sync_counter
    json.dump(counters,file)

rows = conn.execute(
    "SELECT DISTINCT user_id FROM public.plaid_links WHERE status = 'active' ORDER BY user_id ASC"
).fetchall()

user_set_length = len(rows) // balance_sync_user_set_divisor
user_set_start_idx = int((len(rows) * balance_sync_counter)/balance_sync_user_set_divisor)
user_ids = [str(row[0]) for row in rows[user_set_start_idx : user_set_start_idx + user_set_length]]
log_this(f"Running for {user_set_length} users [{user_set_start_idx} -> {user_set_start_idx + user_set_length}]")

def run_on_user(user_id):
    start = datetime.datetime.now()
    log_this(f"Running Balance Sync for {user_id=} ({start})")

    # Get all active user plaid links
    # rows = conn.execute(
    #     f"SELECT DISTINCT id FROM public.plaid_links WHERE user_id = '{user_id}' AND status = 'active'"
    # ).fetchall()
    # plaid_link_ids = [str(row[0]) for row in rows]

    # if len(plaid_link_ids) == 0:
    #     log_this(f"\tNo plaid links for {user_id=}", "warning")
    # else:
        # From the active plaid links, get the most recent plaid transaction set
        # rows = conn.execute(
        #     f"SELECT data FROM public.plaid_raw_transaction_sets WHERE link_id IN {str(tuple(plaid_link_ids)).replace(',)', ')')} ORDER BY created_at DESC LIMIT 10"
        # ).fetchall()


        # # Convert raw transaction set to Pave API account
        # accounts = []
        # for row in rows:
        #     row = row._asdict()['data']
        #     for item in row:
        #         _accounts = item["accounts"]
        #         for account in _accounts:
        #             if account["account_id"] not in [x["account_id"] for x in accounts]:
        #                 log_this(f"\tProcessing account {account['account_id']}")
        #                 accounts.append({
        #                     "account_id": str(account["account_id"]),
        #                     "balances": {
        #                         "available": account["balances"]["available"],
        #                         "current": account["balances"]["current"],
        #                         "iso_currency_code": account["balances"]["iso_currency_code"],
        #                         "limit": account["balances"]["limit"],
        #                         "unofficial_currency_code": account["balances"]["unofficial_currency_code"]
        #                     },
        #                     "mask": account["mask"],
        #                     "name": account["name"],
        #                     "official_name": account["official_name"],
        #                     "type": account["type"],
        #                     "subtype": account["subtype"]
        #                 })

        # log_this(f"\t{user_id=} Got balance_objects: {accounts=}")

        # # Post the account balances to Pave API
        # response = handle_pave_request(
        #     user_id=user_id,
        #     method="post",
        #     endpoint=f"{pave_base_url}/{user_id}/balances",
        #     payload={"run_timestamp": str(datetime.datetime.now()), "accounts": accounts},
        #     headers=pave_headers,
        #     params=None,
        # )

        # if response.status_code == 200:
        # Date ranges for pave, pull for 90 days
    num_balance_days = 90
    start_date_str = (
        datetime.datetime.now() - datetime.timedelta(days=num_balance_days)
    ).strftime("%Y-%m-%d")
    end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d")
    params = {"start_date": start_date_str, "end_date": end_date_str}

    # Get the balance data from Pave API
    response = handle_pave_request(
        user_id=user_id,
        method="get",
        endpoint=f"{pave_base_url}/{user_id}/balances",
        payload=None,
        headers=pave_headers,
        params=params,
    )

    mongo_collection = mongo_db["balances"]
    accounts_balances = response.json().get("accounts_balances", [])

    if len(accounts_balances) > 0:
        mongo_timer = datetime.datetime.now()
        try: # Wrapped in try catch because not passing validation causes errors

            # Timing for future changes, maybe find an aggregation to pull the last 90 or num_balance_days
            find_balance_doc_timer = datetime.datetime.now()
            current_balance_document = mongo_collection.find_one({"user_id": user_id})
            find_balance_doc_timer_end = datetime.datetime.now()
            log_this(f"\tInserting balances for {len(accounts_balances)} accounts.\n\tPulling balances from mongodb took {find_balance_doc_timer_end-find_balance_doc_timer}", "info")

            if not current_balance_document:
                log_this(f"\tCouldn't find any mongo balance record for {user_id=}")
                current_balance_document = {}

            for balance_obj in accounts_balances:

                # The object that stores the combined set of past mongo balances and current Pave API
                # this allows balances in the past {num_balance_days} to be updated
                current_balances = []

                # If the doc exists find the list of balances corresponding with balance_obj account id
                if current_balance_document.get("balances"):
                    current_balances_from_object = [x for x in current_balance_document['balances']['accounts_balances'] if x['account_id'] == balance_obj["account_id"]]
                else:
                    current_balances_from_object = []

                # Should only be one object if the account_id is found, if not take the first one anyways
                # chop the last num_balance_days off and extend current balances with new Pave data
                if len(current_balances_from_object) > 0:
                    if mongo_timer.strftime("%Y-%m-%d") == current_balances_from_object[0]["balances"][-1]["date"]:
                        current_balances = current_balances_from_object[0]['balances'][:-(len(balance_obj["balances"]) + 1)]
                    else:
                        current_balances = current_balances_from_object[0]['balances'][:-(len(balance_obj["balances"]))]

                current_balances.extend(balance_obj["balances"])
                log_this(f"    Total balance days: {len(current_balances)} for account_id={balance_obj['account_id']}")

                # This could fail if there's something incorrect about the balances in current balances
                # like wrong format or if I did a dumb here.
                # The query finds the balances object in balances.accounts_balances with matching account_id
                # and updates it
                update_timer = datetime.datetime.now()
                matched = mongo_collection.update_one(
                    {"user_id": str(user_id), "balances.accounts_balances": {"$elemMatch": {"account_id": balance_obj["account_id"]}}},
                    {
                        "$set":{
                            "balances.accounts_balances.$.balances": current_balances,
                            "balances.accounts_balances.$.days_negative": balance_obj["days_negative"],
                            "balances.accounts_balances.$.days_single_digit": balance_obj["days_single_digit"],
                            "balances.accounts_balances.$.days_double_digit": balance_obj["days_double_digit"],
                            "balances.accounts_balances.$.median_balance": balance_obj["median_balance"]
                        }
                    }
                ).matched_count

                # If there was no balances object with a matching account_id, push this to accounts_balances
                if matched == 0:
                    log_this(f"    Couldn't find account_id={balance_obj['account_id']} in mongo accounts balances, pushing")
                    balance_obj["balances"] = current_balances
                    matched = mongo_collection.update_one(
                        {"user_id": str(user_id)},
                        {
                            "$push":{
                                "balances.accounts_balances": balance_obj,
                            }
                        }
                    ).matched_count

                    if matched == 0:
                        i = mongo_collection.insert_one(
                            {
                                "user_id": user_id,
                                "response_code": 200,
                                "date": now(),
                                "balances": response.json()
                            }
                        ).inserted_id
                        log_this(f"Insterted new doc for {user_id} Inserted id: {i}")
                        print(f"Insterted new doc for {user_id}")
                else:
                    log_this(f"    accounts_balances @ account_id={balance_obj['account_id']} {user_id=}")

                update_timer2 = datetime.datetime.now()
                log_this(f"{user_id} upload balances time took {update_timer2-update_timer} | {'Pushed into accounts_balances' if matched else 'Updated accounts_balance object'}")

            # Update the end date to today
            mongo_collection.update_one(
                {"user_id": str(user_id)},
                {"$set": {"balances.to": end_date_str, "date": datetime.datetime.now()}},
                bypass_document_validation = True
            )
        except Exception as e: # This indicates a validation error
            log_this(f"VALIDATION ERROR ON USER {user_id} ON DAILY SYNC", "error")
            log_this(f"{e}", "error")

        mongo_timer_end = datetime.datetime.now()
        log_this(f"\tDB insertion took: {mongo_timer_end-mongo_timer}", "info")
        # else:
        #     # This happens if the GET balances call to Pave API fails for whatever reason
        #     log_this("\tGot to daily db insertion but no balances were found for the date range", "warning")
        # else:
        #     # This happens if the POST balances (balance upload) call to Pave API fails
        #     log_this("\tCould not upload balances to Pave", "error")

    end = datetime.datetime.now()
    print(f'{user_id} Balance Sync took: {end-start}')

try:
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(run_on_user, user_id) for user_id in user_ids]
        done, incomplete = concurrent.futures.wait(futures)
        log_this(f"Balance Sync: Ran on {len(done)}/{len(user_ids)} users ({len(incomplete)} incomplete)")
except Exception as e:
    print(traceback.format_exception(e))

close_backend_connection()
close_pymongo_connection()

process_end = datetime.datetime.now()
log_this(f"Balance Sync: {process_start} -> {process_end} | Total run time: {process_end-process_start}\n\n\n", "info")
flush_log_buffer()