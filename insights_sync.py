from utils import *

handler = RotatingFileHandler('/home/langston/pave-prism/logs/weekly-recurring-data-sync.log', 'a', (1000**2)*200, 2)
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

process_start = datetime.datetime.now()

log_this("\n\nRuninng unified insights sync:\n", "info")
log_this(f"Process start: {process_start}", "info")

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
for user_id in tqdm(user_ids):

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

close_backend_connection()
close_pymongo_connection()
