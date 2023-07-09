from utils import *

handler = RotatingFileHandler(f'{home_path}logs/weekly-recurring-data-sync.log', 'a', (1000**2)*200, 2)
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

process_start = datetime.datetime.now()

log_this(f"Runinng Unified Insights Sync Process start: {process_start}", "info")

# Open connections
conn = get_backend_connection()
mongo_db = get_pymongo_connection()[pave_table]

rows = conn.execute(
    "SELECT DISTINCT id FROM public.users"
).fetchall()

user_ids = [str(row[0]) for row in rows]

# Date ranges for pave, set for 2 years subject to change
start_date_str = (
    datetime.datetime.now() - datetime.timedelta(days=365*2)
).strftime("%Y-%m-%d")
end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d")
params = {"start_date": start_date_str, "end_date": end_date_str}

# Get all users unified insight data
def run_on_user(user_id):
    start = datetime.datetime.now()
    log_this(f"Running Insights sync for {user_id=}")

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
        for title, object in response.json().items():
            log_this("\tInserting response into: {}".format(title), "info")
            mongo_collection = mongo_db[title]

            try:
                mongo_collection.replace_one(
                    {"user_id": user_id},
                    {
                        title: object,
                        "user_id": user_id,
                        "response_code": response.status_code,
                        "date": datetime.datetime.now(),
                    },
                    upsert=True,
                )
            except Exception as e:
                log_this(f"COULD NOT UPDATE {title} FOR USER {user_id} ON DAILY SYNC", "error")
                log_this(f"{e}", "error")
    else:
        log_this("\tCan't insert: {} {}\n".format(response.status_code, response.json()), "warning")
    #####################################################################

    # We may actually want this data for decisioning so we can take the slowdown
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
    end = datetime.datetime.now()
    print(f'{user_id} Insights Sync took: {end-start}')

with concurrent.futures.ThreadPoolExecutor(10) as executor:
    futures = [executor.submit(run_on_user, user_id) for user_id in user_ids]
    done, incomplete = concurrent.futures.wait(futures)
    log_this(f"Insights sync: Ran on {len(done)}/{len(user_ids)} users ({len(incomplete)} incomplete)")

close_backend_connection()
close_pymongo_connection()

process_end = datetime.datetime.now()
log_this(f"Unified Insights Sync: {process_start} -> {process_end} | Total run time: {process_end-process_start}\n\n\n", "info")
