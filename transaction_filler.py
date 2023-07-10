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
    log_this(f"  Pave Agent res code: {res.status_code}, took {pave_agent_end-pave_agent_start} | {res.json()=}")

def main():
    process_start = now()

    log_this(f"Runinng Transaction Filler Process start: {process_start}\n", "info")

    # Open connections
    conn = get_backend_connection()

    rows = conn.execute(
        "SELECT DISTINCT access_token, user_id FROM public.plaid_links WHERE status = 'active'"
    ).fetchall()

    with concurrent.futures.ThreadPoolExecutor(10) as executor:
        futures = [executor.submit(run_on_user, row) for row in rows]
        done, incomplete = concurrent.futures.wait(futures)
        log_this(f"Transaction Filler: Ran on {len(done)}/{len(rows)} users ({len(incomplete)} incomplete)")

    close_backend_connection()

    process_end = now()
    log_this(f"Transaction Filler: {process_start} -> {process_end} | Total run time: {process_end-process_start}\n\n\n", "info")
    flush_log_buffer()

if __name__ == "__main__":
    main()
