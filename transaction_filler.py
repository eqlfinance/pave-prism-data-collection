from utils import *


def run_on_user(at_uid):
    at_uid = at_uid._asdict()
    access_token, user_id = decrypt(at_uid["access_token"]), str(at_uid["user_id"])
    time_in_days = 90

    log_this(f"Filling {time_in_days} days of transactions for {user_id=}")
    pave_agent_start = now()
    res = requests.post(
        f"http://127.0.0.1:8123/v1/users/{user_id}/upload?num_transaction_days={time_in_days}",
        json={"access_token": f"{access_token}"},
    )
    pave_agent_end = now()
    log_this(
        f"  Pave Agent res code: {res.status_code}, took {pave_agent_end-pave_agent_start} | {res.json()=}"
    )


def main():
    handler = RotatingFileHandler(
        f"{logs_path}transaction_filler.log", "a+", (1000**2) * 200, 2
    )
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    process_start = now()

    log_this(f"Runinng Transaction Filler Process start: {process_start}\n", "info")

    # Open connections
    conn = get_backend_connection()
    
    # This is the way we run only on a set of users so as the user set exapnds
    # the speed doesn't become unreasonable
    transaction_filler_user_set_divisor = counters["transaction_filler_usd"]
    transaction_filler_counter = counters["transaction_filler_counter"]
    transaction_filler_counter = (transaction_filler_counter + 1) % transaction_filler_user_set_divisor

    with open(f"{home_path}counters.json", "w") as file:
        counters["transaction_filler_counter"] = transaction_filler_counter
        json.dump(counters, file)

    rows = conn.execute(
        "SELECT DISTINCT access_token, user_id FROM public.plaid_links WHERE status = 'active' ORDER BY user_id ASC"
    ).fetchall()

    user_set_length = len(rows) // transaction_filler_user_set_divisor
    user_set_start_idx = int(
        (len(rows) * transaction_filler_counter) / transaction_filler_user_set_divisor
    )
    rows = [
        row
        for row in rows[user_set_start_idx : user_set_start_idx + user_set_length]
    ]
    log_this(
        f"Running for {user_set_length} users, indexes [{user_set_start_idx} -> {user_set_start_idx + user_set_length}] (ORDER BY ASC)",
        "warning"
    )

    with concurrent.futures.ThreadPoolExecutor(20) as executor:
        futures = [executor.submit(run_on_user, row) for row in rows]
        done, incomplete = concurrent.futures.wait(futures)
        log_this(
            f"Transaction Filler: Ran on {len(done)}/{len(rows)} users ({len(incomplete)} incomplete)"
        )

    close_backend_connection()

    process_end = now()
    log_this(
        f"Transaction Filler: {process_start} -> {process_end} | Total run time: {process_end-process_start}",
        "info"
    )
    flush_log_buffer()
    logger.info("\n\n\n")


if __name__ == "__main__":
    main()
