from utils import *

#at_uid: the Access Token - User Id pair from plaid links
def run_on_user(at_uid):
    mongo_db = get_pymongo_connection()
    at_uid = at_uid._asdict()
    access_token, user_id = decrypt(at_uid["access_token"]), str(at_uid["user_id"])

    start = now()
    log_this(f"**** Running new link sync for {user_id=} ****")
    time_in_days = 365 * 2

    # Upon creation of the webhook we could store the request_id from this
    requests.post(
        f"http://127.0.0.1:8123/v1/users/{user_id}/upload?num_transaction_days={time_in_days}",
        json={"access_token": f"{access_token}"},
    )

    # Give pave agent some time to process transactions
    time.sleep(5)

    # Date ranges for pave
    start_date_str = (
        now() - datetime.timedelta(days=time_in_days)
    ).strftime("%Y-%m-%d")
    end_date_str: str = now().strftime("%Y-%m-%d")
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
        log_this(f"        Could not get {user_id} transactions")
    else:
        mongo_timer = now()
        mongo_collection = mongo_db["transactions"]
        transactions = response.json()["transactions"]

        if len(transactions) > 0:
            log_this(f"   Inserting {len(transactions)} transactions for user {user_id}", "info")

            try:
                mongo_collection.update_one(
                    {"user_id": str(user_id)},
                    {
                        "$addToSet": {"transactions.transactions": {"$each": transactions}},
                        "$set": {"transactions.to": end_date_str, "date": now()}
                    }
                )
            except Exception as e:
                log_this(f"        COULD NOT UPDATE TRANSACTIONS FOR USER {user_id} ON LINK SYNC", "error")
                log_this(f"        {''.join(traceback.format_exception(e))}", "error")

            mongo_timer_end = now()
            log_this(f"      Transaction insertion took: {mongo_timer_end-mongo_timer}", "info")

            transaction_date_str = transactions[len(transactions)-1]["date"]
            params["start_date"] = transaction_date_str
        else:
            log_this("\tGot to new link transaction db insertion but no transactions were found for the date range")

    #####################################################################

    # Store the balance data from pave
    response = handle_pave_request(
        user_id=user_id,
        method="get",
        endpoint=f"{pave_base_url}/{user_id}/balances",
        payload=None,
        headers=pave_headers,
        params=params,
    )

    if response.status_code != 200:
        log_this(f"    Balance Get for {user_id} failed")
    else:
        mongo_timer = now()
        mongo_collection = mongo_db["balances2"]

        log_this(f"    Moving account data for {user_id}'s accounts {[x['account_id'] for x in balance_obj['accounts_balances']]} into balances", "info")

        try:
            accounts_balances = response.json().get("accounts_balances", [])
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
                    balance['pulled_date'] = mongo_timer

                    bulk_writes.append(pymongo.ReplaceOne({"user_id": user_id, "account_id": account_id, "date": balance['date']}, replacement=balance, upsert=True))

            update_timer = now()
            mongo_collection.bulk_write(bulk_writes)
            update_timer2 = now()
            log_this(f"    {user_id=} bulk writes to balances took {update_timer2-update_timer}, accounts balances processing took {update_timer-ab_processing}: {account_ids=}")
        except Exception as e:
            log_this(f"    COULD NOT UPDATE BALANCES FOR USER {user_id} ON LINK SYNC", "error")
            log_this("\n".join(traceback.format_exception(e)), "error")

        mongo_timer_end = now()
        log_this(f"    {user_id} Balance insertion took: {mongo_timer_end-mongo_timer}", "info")

    end = now()
    log_this(f'**** {user_id} Balance Sync took: {end-start} ****')

def main():
    handler = RotatingFileHandler(f'{home_path}new-link-data-sync.log', 'a', (1000**2)*200, 2)
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    process_start = now()

    log_this(f"Runinng new link sync Process start: {process_start}\n", "warning")

    # Open connections
    conn = get_backend_connection()

    rows = conn.execute(
        "SELECT DISTINCT access_token, user_id FROM public.plaid_links WHERE created_at >= (NOW() - INTERVAL '30 minutes') OR last_validated_at >= (NOW() - INTERVAL '3 days')"
    ).fetchall()

    with concurrent.futures.ThreadPoolExecutor(10) as executor:
        futures = [executor.submit(run_on_user, row) for row in rows]
        done, incomplete = concurrent.futures.wait(futures)
        log_this(f"New Links Sync sync: Ran on {len(done)}/{len(rows)} users ({len(incomplete)} incomplete)", "warning")

    close_backend_connection()
    close_pymongo_connection()

    process_end = now()
    log_this(f"New Links Sync: {process_start} -> {process_end} | Total run time: {process_end-process_start}", "warning")
    log_this("\n\n\n")
    flush_log_buffer()

if __name__ == "__main__":
    main()
