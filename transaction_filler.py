from utils import *

conn = get_backend_connection()

rows = conn.execute(
    "SELECT DISTINCT access_token, user_id FROM public.plaid_links WHERE status = 'active'"
).fetchall()

for row in tqdm(rows):
    row = row._asdict()
    access_token, user_id = decrypt(row["access_token"]), str(row["user_id"])
    time_in_days = 365 * 4

    print(f"{user_id=}")
    pave_agent_start = datetime.datetime.now()
    res = requests.post(
        f"http://127.0.0.1:8123/v1/users/{user_id}/upload?num_transaction_days={time_in_days}",
        json={"access_token": f"{access_token}"},
    )
    pave_agent_end = datetime.datetime.now()
    print(f"  Pave Agent res code: {res.status_code}, took {pave_agent_end-pave_agent_start}\n        {res.json()}")
