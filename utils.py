import base64
import datetime
import json
import logging
import subprocess
import time
import uuid
import pymongo
import requests
import sqlalchemy
from tqdm import tqdm
import concurrent.futures
import os
from logging.handlers import RotatingFileHandler
from cryptography.fernet import Fernet, MultiFernet

from google.cloud.sql.connector import Connector
from google.oauth2 import service_account
from google.cloud import secretmanager


logger = logging.getLogger("stevenslav2")
logger.setLevel(logging.DEBUG)

home_path = "/home/langston/pave-prism/"
counters = None

# Get the counters data. This allows for runing on smaller sets of users
if not os.path.isfile(f"{home_path}counters.json"):
    with open(f'{home_path}counters.json', 'w') as file:
        default_counter_values = {"balance_sync_counter": 0,"balance_sync_usd": 6}
        json.dump(default_counter_values, file)
        counters = default_counter_values

if counters is None:
    with open(f'{home_path}counters.json', 'r') as file:
        counters = json.load(file)

proc_id = str(uuid.uuid4())[:8]
formatter = logging.Formatter(f'{proc_id} [%(levelname)s] @ %(asctime)s: %(message)s', datefmt='%m-%d %H:%M:%S')

normal_log_handler = RotatingFileHandler(f'{home_path}stevenslav2.log', 'a+', 1000**3, 2)
normal_log_handler.setFormatter(formatter)
normal_log_handler.setLevel(logging.DEBUG)
logger.addHandler(normal_log_handler)

# Get secret values
secret_manager_client = secretmanager.SecretManagerServiceClient()

# Decrpytion keys
keys = secret_manager_client.access_secret_version(
    name=f"projects/eql-data-processing-stage/secrets/pave-agent-decryption-keys/versions/latest"
).payload.data.decode("UTF-8")
keys = json.loads(keys)["KEYS"]

# Pave necessities
pave_str = secret_manager_client.access_secret_version(
    name=f"projects/eql-data-processing-stage/secrets/pave-prism-info/versions/latest"
).payload.data.decode("UTF-8")

pave_data = json.loads(pave_str)
pave_base_url = pave_data["PAVE_HOST"]
pave_x_api_key = pave_data["PAVE_X_API_KEY"]
pave_headers = {
    "Content-Type": "application/plaid+json",
    "x-api-key": pave_x_api_key,
}
pave_table = "pave-stage"

# Backend connection necessities
client = secretmanager.SecretManagerServiceClient()

CREDS = client.access_secret_version(
    name=f"projects/eql-data-processing-stage/secrets/eql-backend-service-stage-creds/versions/latest"
).payload.data.decode("UTF-8")

creds_obj = json.loads(CREDS)

DB_PARAMS = client.access_secret_version(
    name=f"projects/eql-data-processing-stage/secrets/eql-backend-service-stage-db/versions/latest"
).payload.data.decode("UTF-8")

db_params_obj = json.loads(DB_PARAMS)

g_credentials = service_account.Credentials.from_service_account_info(creds_obj)
instance_connection_name = f"{db_params_obj['PROJECT_ID']}:{db_params_obj['REGION']}:{db_params_obj['INSTANCE_NAME']}"

def get_psql_connection():
    global g_credentials, instance_connection_name, db_params_obj

    connector = Connector(credentials=g_credentials)
    conn = connector.connect(
        instance_connection_name,
        "pg8000",
        user=db_params_obj["DB_USER"],
        password=db_params_obj["DB_PASS"],
        db=db_params_obj["TABLE"],
    )

    return conn

postgres_pool = sqlalchemy.create_engine(
    "postgresql+pg8000://", creator=get_psql_connection
)

mongodb_uri = client.access_secret_version(
    name=f"projects/eql-data-processing-stage/secrets/mongodb-uri/versions/latest"
).payload.data.decode("UTF-8")

current_mongo_connection:pymongo.MongoClient = None
def get_pymongo_connection() -> pymongo.MongoClient:
    global current_mongo_connection

    if current_mongo_connection: return current_mongo_connection
    else:
        current_mongo_connection = pymongo.MongoClient(mongodb_uri)
        return current_mongo_connection

def close_pymongo_connection():
    global current_mongo_connection
    if current_mongo_connection: current_mongo_connection.close()

current_backend_connection:sqlalchemy.engine.Connection = None
def get_backend_connection() -> sqlalchemy.engine.Connection:
    global current_backend_connection

    if current_backend_connection: return current_backend_connection
    else:
        current_backend_connection = postgres_pool.connect()
        return current_backend_connection

def close_backend_connection():
    global current_backend_connection
    if current_backend_connection: current_backend_connection.close()

def log_this(message:str, severity:str = "debug"):
    # Logging to a local file and gcloud
    global logger
    logger.log(logging._nameToLevel[severity.upper()], message)
    subprocess.run(["gcloud", "logging", "write", "stevenslav", message, f"--severity={severity.upper()}", "--quiet", "--verbosity=none", "--no-user-output-enabled"], stdout=subprocess.PIPE)

def base64_decode(val: str) -> bytes:
    return base64.urlsafe_b64decode(val.encode("ascii"))


def decrypt(val: str) -> str:
    if val is None:
        return None

    fernet = MultiFernet(Fernet(k) for k in keys)
    actual = fernet.decrypt(base64_decode(val))
    return actual.decode()


def handle_pave_request(
    user_id: str,
    method: str,
    endpoint: str,
    payload: dict,
    headers: dict,
    params: dict,
    last_wait: float = 0,
) -> requests.Response:
    request_timer = datetime.datetime.now()

    if method.lower().strip() in ["get", "post"]:
        res = requests.request(method.upper().strip(), f"{endpoint}", json=payload, headers=headers, params=params)
    else:
        raise ValueError("Method not understood {}".format(method))

    res_code = res.status_code

    if res_code == 429: # Rate limit!
        sleep = 1 if last_wait == 0 else last_wait * 2
        log_this(f"Request limit reached, waiting {sleep} seconds", "error")
        time.sleep(sleep)
        return handle_pave_request(
            user_id, method, endpoint, payload, headers, params, sleep
        )
    else:
        request_timer_end = datetime.datetime.now()
        log_this(f"[Response {res_code}] {method.upper()} {endpoint} took: {request_timer_end-request_timer}", "info")

        return res

def insert_response_into_db(
    user_id: str, res, mongo_db, collection_name: str, response_column_name: str
):
    log_this("Inserting response into: {}.{}".format(collection_name, response_column_name), "info")
    mongo_timer = datetime.datetime.now()
    mongo_collection = mongo_db[collection_name]
    res_code = res.status_code

    if res_code == 200:
        try:
            res = mongo_collection.replace_one(
                {"user_id": user_id},
                {
                    response_column_name: res.json(),
                    "user_id": user_id,
                    "response_code": res.status_code, #depriciated 
                    "date": datetime.datetime.now(),
                },
                upsert=True
            )

            log_this(f"        {res.matched_count=} {res.modified_count=} {res.upserted_id=}")
            if res.matched_count == 0 and res.modified_count == 0 and res.upserted_id == None:
                raise Exception("Impossible case")
        except Exception as e:
            # Most likely a validation error happened
            log_this(f"COULD NOT INSERT response into {response_column_name} FOR USER {user_id}", "error")
            log_this(f"{e}", "error")
    else: 
        # No insertion when response code is 400. There was probably a plaid error that prevented
        # user data from going to Pave so the request to Pave API did not work
        log_this("\tCan't insert to {}: {} {}\n".format(collection_name, res_code, res.json()), "warning")

    mongo_timer_end = datetime.datetime.now()
    log_this(f"DB insertion to {collection_name} took: {mongo_timer_end-mongo_timer}\n", "warning")
