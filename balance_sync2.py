from utils import *

def run_on_user(user_id, mongo_db):
    start = datetime.datetime.now()
    log_this(f"**** Running Balance Sync for {user_id=} ({start}) ****")

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
            log_this(f"    Inserting balances for {len(accounts_balances)} accounts ({user_id=})", "info")

            for balance_obj in accounts_balances:
                # The object that stores the combined set of past mongo balances and current Pave API
                # this allows balances in the past {num_balance_days} to be updated
                current_balances = balance_obj['balances']
                account_id = balance_obj['account_id']

                log_this(f"    Total balance days: {len(current_balances)} for {account_id=}")

                update_timer = datetime.datetime.now()

                bulk_writes = []
                for balance in current_balances:
                    balance['user_id'] = user_id
                    balance['account_id'] = account_id

                    bulk_writes.append(pymongo.ReplaceOne({"user_id": user_id, "account_id": balance_obj['account_id'], "date": balance['date']}, replacement=balance, upsert=True))

                mongo_collection["balances2"].bulk_write(bulk_writes)
                update_timer2 = datetime.datetime.now()
                log_this(f"{user_id} upload balances time took {update_timer2-update_timer}")
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

    with concurrent.futures.ProcessPoolExecutor(10, max_tasks_per_child=10) as executor:
        futures = [executor.submit(run_on_user, user_id, mongo_db) for user_id in user_ids]
        concurrent.futures.wait(futures)

    close_backend_connection()
    close_pymongo_connection()

    process_end = datetime.datetime.now()
    log_this(f"Balance Sync: {process_start} -> {process_end} | Total run time: {process_end-process_start}\n\n\n", "info")


if __name__ == "__main__":
    main()
