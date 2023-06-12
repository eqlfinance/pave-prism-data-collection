from utils import *

handler = RotatingFileHandler('/home/langston/pave-prism/logs/new-user-data-sync.log', 'a+', (1000**2)*200, 2)
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

process_start = datetime.datetime.now()

log_this("\n\nRuninng new user sync:\n", "info")
log_this(f"Process start: {process_start}", "info")

conn = get_backend_connection()
mongo_db = get_pymongo_connection()[pave_table]

rows = conn.execute(
    "SELECT DISTINCT id FROM public.users WHERE created_at >= (NOW() - INTERVAL '10 hours')"
).fetchall()

# Ensure that we only get user_ids that we haven't processed before
# because this is an expensive sync
pave_user_ids = [x["user_id"] for x in list(mongo_db["balances"].find({}))]
user_ids = [str(row[0]) for row in rows if str(row[0]) not in pave_user_ids]

start = datetime.datetime.now()
# Get all user access tokens and upload transaction/balance them using the pave agent
for user_id in tqdm(user_ids):
    loop_start = datetime.datetime.now()
    rows = conn.execute(
        f"SELECT DISTINCT access_token FROM public.plaid_links WHERE user_id = '{user_id}'"
    ).fetchall()

    access_tokens = [decrypt(str(row[0])) for row in rows]
    time_in_days = 365 * 2

    for access_token in access_tokens:
        requests.post(
            f"http://127.0.0.1:8123/v1/users/{user_id}/upload?num_transaction_days={time_in_days}",
            json={"access_token": f"{access_token}"},
        )
        # Give pave agent some time to process transactions
        time.sleep(2)

    # Date ranges for pave
    start_date_str = (
        datetime.datetime.now() - datetime.timedelta(days=time_in_days)
    ).strftime("%Y-%m-%d")
    end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d")
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
    insert_response_into_db(
        user_id=user_id,
        res=response,
        mongo_db=mongo_db,
        collection_name="transactions",
        response_column_name="transactions",
    )
    if response.status_code == 200:
        transactions = response.json()["transactions"]
        if len(transactions) > 0:
            transaction_date_str = transactions[len(transactions)-1]["date"]
            params["start_date"] = transaction_date_str
    #####################################################################

    response = handle_pave_request(
        user_id=user_id,
        method="get",
        endpoint=f"{pave_base_url}/{user_id}/balances",
        payload=None,
        headers=pave_headers,
        params=params,
    )
    insert_response_into_db(
        user_id=user_id,
        res=response,
        mongo_db=mongo_db,
        collection_name="balances",
        response_column_name="balances",
    )
    #####################################################################

    # Store the unified insights data from pave
    params = {
        "start_date": start_date_str,
        "end_date": end_date_str,
        "with_transactions": True,
    }
    response = handle_pave_request(
        user_id=user_id,
        method="get",
        endpoint=f"{pave_base_url}/{user_id}/unified_insights",
        payload=None,
        headers=pave_headers,
        params=params,
    )

    if response.status_code == 200:
        ui_start = datetime.datetime.now()
        for title, obj in response.json().items():
            log_this("\tInserting response into: {}".format(title), "info")
            mongo_collection = mongo_db[title]

            try:
                mongo_collection.replace_one(
                    {"user_id": user_id},
                    {
                        title: obj,
                        "user_id": user_id,
                        "response_code": response.status_code,
                        "date": datetime.datetime.now(),
                    },
                    upsert=True,
                )
            except Exception as e:
                log_this(f"COULD NOT INSERT {title} FOR USER {user_id} ON NEW USER SYNC", "error")
                log_this(f"{e}", "error")
        ui_end = datetime.datetime.now()
        log_this(f" Unified insights entry took {ui_end-ui_start}")

    else:
        log_this("\tCan't insert: {} {}\n".format(response.status_code, response.json()), "warning")
    #####################################################################

    # Store the attribute data from pave
    params = {"date": end_date_str}
    response = handle_pave_request(
        user_id=user_id,
        method="get",
        endpoint=f"{pave_base_url}/{user_id}/attributes",
        payload=None,
        headers=pave_headers,
        params=params,
    )
    insert_response_into_db(
        user_id=user_id,
        res=response,
        mongo_db=mongo_db,
        collection_name="attributes",
        response_column_name="attributes",
    )
    #####################################################################
    finish = datetime.datetime.now()

    log_this(f"    > Loop time took {finish-loop_start}")
    # If this has taken 4 hours it probably got stuck somewhere
    if finish - start > datetime.timedelta(hours=4):
        break

close_backend_connection()
close_pymongo_connection()
