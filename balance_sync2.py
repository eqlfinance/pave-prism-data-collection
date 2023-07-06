from utils import *


def run_on_user(user_id, mongo_db):
    start = datetime.datetime.now()
    log_this(f"**** Running Balance Sync for {user_id=} ({start}) ****")

    num_balance_days = 30
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
            current_balance_document = mongo_collection.find_one({"user_id": user_id})
            log_this(f"    Inserting balances for {len(accounts_balances)} accounts ({user_id=})", "info")

            # If the doc exists find the list of balances corresponding with balance_obj account id
            if current_balance_document is not None and current_balance_document.get("balances"):
                current_accounts_balances_obj_from_doc = {x['account_id']:x for x in current_balance_document['balances']['accounts_balances']}
            else:
                current_accounts_balances_obj_from_doc = {}

            for balance_obj in accounts_balances:
                # The object that stores the combined set of past mongo balances and current Pave API
                # this allows balances in the past {num_balance_days} to be updated
                current_balances = []

                # If the account_id of the balance_obj from Pave API matches one from MongoDB, then we
                # have to update the account_balances list from that document
                current_accounts_balances_obj = current_accounts_balances_obj_from_doc.get(balance_obj['account_id'])
                if current_accounts_balances_obj:
                    # Search for the balance on the start_date of the range on the account
                    b = [balance for balance in current_accounts_balances_obj['balances'] if balance['date'] == start_date_str]
                    if len(b) > 0:
                        # If the start_date is in the range of the balances currently in mongodb, we're splicing the
                        # Pave API balances in from start_date to end_date aka the length of balance_obj['balances']
                        doc_balance_on_start_date = b[0]
                        split_idx = current_accounts_balances_obj['balances'].index(doc_balance_on_start_date)

                        log_this(f"    Splicing balances: {start_date_str} -> \n{split_idx=} {len(current_accounts_balances_obj['balances'])=}\n{current_accounts_balances_obj['balances'][max(split_idx-1,0)]=} {balance_obj['balances'][0]=}  {split_idx+len(balance_obj['balances'])=}", "info")

                        # Splicing func
                        current_balances = current_accounts_balances_obj['balances'][:split_idx]
                        current_balances.extend(balance_obj["balances"])
                        current_balances.extend(current_accounts_balances_obj['balances'][split_idx+len(balance_obj['balances']):])
                    else:
                        # If the date can't be found, the add the Pave API balances to the end
                        # of the mongo balances
                        current_balances = current_accounts_balances_obj['balances']
                        current_balances.extend(balance_obj["balances"])
                else:
                    current_balances = balance_obj['balances']

                log_this(f"    Total balance days: {len(current_balances)} for account_id={balance_obj['account_id']}")

                # This could fail if there's something incorrect about the balances in current balances
                # like wrong format or if I did a dumb here.
                # The query finds the balances object in balances.accounts_balances with matching account_id
                # and updates it
                update_timer = datetime.datetime.now()
                matched = 1
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
                    mongo_collection.update_one(
                        {"user_id": str(user_id)},
                        {
                            "$push":{
                                "balances.accounts_balances": balance_obj,
                            }
                        }
                    )
                else:
                    log_this(f"    accounts_balances @ account_id={balance_obj['account_id']} {user_id=}")

                update_timer2 = datetime.datetime.now()
                log_this(f"{user_id} upload balances time took {update_timer2-update_timer} | {'Pushed into accounts_balances' if matched == 0 else 'Updated accounts_balance object'}")

            #Update the end date to today
            mongo_collection.update_one(
                {"user_id": str(user_id)},
                {"$set": {"balances.to": end_date_str, "date": datetime.datetime.now()}},
                bypass_document_validation = True
            )
        except Exception as e: # This indicates a validation error
            log_this(f"VALIDATION ERROR ON USER {user_id} ON DAILY SYNC", "error")
            logger.exception(e)

        mongo_timer_end = datetime.datetime.now()
        log_this(f"\tDB insertion took: {mongo_timer_end-mongo_timer}", "info")
    else:
        # This happens if the GET balances call to Pave API fails for whatever reason
        log_this("\tGot to daily db insertion but no balances were found for the date range", "warning")

    end = datetime.datetime.now()
    print(f'**** {user_id} Balance Sync took: {end-start} ****')

def main():
    handler = RotatingFileHandler(f'{home_path}daily-balance-data-sync.log', 'a+', (1000**2)*200, 2)
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
        "SELECT DISTINCT id FROM public.users ORDER BY id ASC"
    ).fetchall()

    user_set_length = len(rows) // balance_sync_user_set_divisor
    user_set_start_idx = int((len(rows) * balance_sync_counter)/balance_sync_user_set_divisor)
    user_ids = [str(row[0]) for row in rows[user_set_start_idx : user_set_start_idx + user_set_length]]
    log_this(f"Running for {user_set_length} users [{user_set_start_idx} -> {user_set_start_idx + user_set_length}]")

    with concurrent.futures.ThreadPoolExecutor(10) as executor:
        futures = [executor.submit(run_on_user, user_id, mongo_db) for user_id in user_ids]
        concurrent.futures.wait(futures)

    close_backend_connection()
    close_pymongo_connection()

    process_end = datetime.datetime.now()
    log_this(f"Balance Sync: {process_start} -> {process_end} | Total run time: {process_end-process_start}\n\n\n", "info")


if __name__ == "__main__":
    main()
