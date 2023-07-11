from utils import *

def run_on_user(user_id):
    mongo_collection = get_pymongo_connection()[pave_table]["balances2"]

    start = now()
    log_this(f"**** Running Balance Sync for {user_id=} ({start}) ****")

    num_balance_days = 90
    start_date_str = (
        now() - datetime.timedelta(days=num_balance_days)
    ).strftime("%Y-%m-%d")
    end_date_str: str = now().strftime("%Y-%m-%d")
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


    if response.status_code == 200:
        accounts_balances = response.json().get("accounts_balances", [])
        try: # Wrapped in try catch because not passing validation causes errors
            log_this(f"    Inserting balances for {len(accounts_balances)} accounts ({user_id=})", "info")

            bulk_writes = []
            account_ids = []
            ab_processing = now()
            for balance_obj in accounts_balances:
                # The object that stores the combined set of past mongo balances and current Pave API
                # this allows balances in the past {num_balance_days} to be updated
                current_balances = balance_obj['balances']
                account_id = balance_obj['account_id']
                account_ids.append(account_id)

                for balance in current_balances:
                    balance['user_id'] = user_id
                    balance['account_id'] = account_id
                    balance['pulled_date'] = start

                    bulk_writes.append(pymongo.ReplaceOne({"user_id": user_id, "account_id": account_id, "date": balance['date']}, replacement=balance, upsert=True))

            update_timer = now()
            mongo_collection.bulk_write(bulk_writes)
            update_timer2 = now()
            log_this(f"    {user_id=} bulk writes to balances took {update_timer2-update_timer}, accounts balances processing took {update_timer-ab_processing}: {account_ids=}")
        except Exception as e: # This indicates a validation error
            log_this(f"MONGO DB OR OTHER ERROR ON USER {user_id} ON DAILY SYNC", "error")
            log_this("\n".join(traceback.format_exception(e)), "error")

    else:
        # This happens if the GET balances call to Pave API fails for whatever reason
        log_this(f"        {user_id}: balances call failed for date range {start_date_str}-{end_date_str}", "warning")

    end = now()
    log_this(f'**** {user_id} Balance Sync took: {end-start} ****')

def main():
    handler = RotatingFileHandler(f'{logs_path}daily-balance-data-sync.log', 'a+', (1000**2)*200, 2)
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    process_start = now()

    log_this(f"Runinng Balance Sync Process start: {process_start}", "warning")

    conn = get_backend_connection()
    #mongo_db = get_pymongo_connection()[pave_table]

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
    log_this(f"Running for {user_set_length} users, indexes [{user_set_start_idx} -> {user_set_start_idx + user_set_length}] (ORDER BY ASC)", 'warning')

    with concurrent.futures.ThreadPoolExecutor(user_set_length) as executor:
        futures = [executor.submit(run_on_user, user_id) for user_id in user_ids]
        concurrent.futures.wait(futures)

    close_backend_connection()
    close_pymongo_connection()

    process_end = now()
    log_this(f"Balance Sync {user_set_length} users: {process_start} -> {process_end} | Total run time: {process_end-process_start}", "warning")
    log_this("\n\n\n")
    flush_log_buffer()




if __name__ == "__main__":
    main()
