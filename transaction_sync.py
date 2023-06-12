from utils import *

handler = RotatingFileHandler('/home/langston/pave-prism/logs/hourly-transaction-data-sync.log', 'a', (1000**2)*200, 2)
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

process_start = datetime.datetime.now()

log_this("\n\nRuninng transaction sync:\n", "info")
log_this(f"Process start: {process_start}", "info")

# Open connections
conn = get_backend_connection()
mongo_db = get_pymongo_connection()[pave_table]

rows = conn.execute(
    "SELECT * FROM public.plaid_transactions WHERE plaid_transactions.date >= (NOW() - INTERVAL '10 hours')"
).fetchall()

for row in tqdm(rows):
    row:dict = row._asdict()
    transaction = {
        "transaction_id": str(row["plaid_transaction_id"]),
        "account_id": str(row["plaid_account_id"]),
        "amount": float(row["amount"]),
        "date": str(row["authorized_date"]),
        "memo": " ".join(
            row["personal_finance_category"].values()
        )
        if row["personal_finance_category"]
        else "",
        "name": row["name"] if row["name"] else " ",
        "pending": row["pending"],
        "category": row["category"],
        "category_id": row["category_id"],
        "iso_currency_code": row["iso_currency_code"],
        "merchant_name": row["merchant_name"],
        "payment_channel": row["payment_channel"],
        "transaction_type": row["transaction_type"],
        "payment_meta": row["payment_meta"],
        "location": row["location"]
    }

    user_id = conn.execute(f"SELECT user_id FROM public.plaid_links WHERE id = \'{str(row['link_id'])}\'").fetchone()[0]

    # Date ranges for pave
    start_date_str = (
        datetime.datetime.now() - datetime.timedelta(days=1)
    ).strftime("%Y-%m-%d")
    end_date_str: str = datetime.datetime.now().strftime("%Y-%m-%d")
    params = {"start_date": start_date_str, "end_date": end_date_str, "resolve_duplicates": True}

    response = handle_pave_request(
        user_id=user_id,
        method="post",
        endpoint=f"{pave_base_url}/{user_id}/transactions",
        payload={"transactions": [transaction]},
        headers=pave_headers,
        params=params,
    )

    #####################################################################

    if response.status_code == 200:
        # Store the transaction data from pave
        response = handle_pave_request(
            user_id=user_id,
            method="get",
            endpoint=f"{pave_base_url}/{user_id}/transactions",
            payload=None,
            headers=pave_headers,
            params=params,
        )

        mongo_timer = datetime.datetime.now()
        mongo_collection = mongo_db["transactions"]
        transactions = response.json()["transactions"]

        if len(transactions) > 0:
            log_this(f"\tInserting {json.dumps(transactions)[:100]} into transactions", "info")

            mongo_collection.update_one(
                {"user_id": str(user_id)},
                {
                    "$addToSet": {"transactions.transactions": {"$each": transactions}},
                    "$set": {"transactions.to": end_date_str, "date": datetime.datetime.now()}
                }
            )

            mongo_timer_end = datetime.datetime.now()
            log_this(f"\tDB insertion took: {mongo_timer_end-mongo_timer}", "info")
        else:
            log_this("\tGot to hourly db insertion but no transactions were found for the date range", "warning")
    else:
        log_this("Could not upload transactions to mongodb", "error")


close_backend_connection()
close_pymongo_connection()
