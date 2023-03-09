
import base64
import datetime
from typing import List
import json
from google.cloud import secretmanager
from tqdm import tqdm
import logging
from db_connections import Connection_Manager
from cryptography.fernet import Fernet, MultiFernet
import subprocess

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/langston/Box/Coding/EQL/pave-prism-int/eql-data-processing-creds.json"

secret_manager_client = secretmanager.SecretManagerServiceClient()

keys = secret_manager_client.access_secret_version(
    name=f"projects/eql-data-processing/secrets/pave-agent-decryption-keys/versions/latest"
).payload.data.decode("UTF-8")
keys=json.loads(keys)['KEYS']

def base64_decode(val: str) -> bytes:
    return base64.urlsafe_b64decode(val.encode("ascii"))

def decrypt(val: str) -> str:
    fernet = MultiFernet(Fernet(k) for k in keys)
    actual = fernet.decrypt(base64_decode(val))
    return actual.decode()


def run(user_ids:List[str] = []) -> bool:
    
    # Set up the logging instance
    # TODO: This overwrites previous logs on each run
    #logging.basicConfig(
    #    handlers = [RotatingFileHandler("stevenslav.log", maxBytes=1024 ** 3, backupCount=2, mode='w')],
    #    format="%(name)s @ %(asctime)s: %(message)s",
    #    datefmt="%I:%M:%S",
    #    level=logging.DEBUG,
    #)

    cm = Connection_Manager()
    process_start = datetime.datetime.now()

    try:
        with open("/home/langston/pave-prism/pave-agent/one-time-run.env", mode='w') as env_file:    
            #user_ids = []
            # Open connection to postgres db
            conn = cm.get_postgres_connection()

            # Calculate for users who are new like within the last 2 hours
            #rows = conn.execute("SELECT DISTINCT id FROM public.users ").fetchall()
            #user_ids = [str(row[0]) for row in rows]
            
            if len(user_ids) < 1:
                # Close db connection
                cm.close_postgres_connection(conn)
                
                process_end = datetime.datetime.now()
                logging.info(f"No new users found. Ending process")
                logging.info(f"\nTotal runtime: {process_end-process_start}")
                return True
                
            # Get pave secret values
            #pave_str = secret_manager_client.access_secret_version(
            #    name=f"projects/eql-data-processing/secrets/pave-prism-info/versions/latest"
            #).payload.data.decode("UTF-8")

            #pave_data = json.loads(pave_str)
            #pave_base_url = pave_data["PAVE_HOST"]
            #pave_x_api_key = pave_data["PAVE_X_API_KEY"]
            #pave_headers = {
            #    "Content-Type": "application/plaid+json",
            #    "x-api-key": pave_x_api_key,
            #}
            
            
            env_file_start = secret_manager_client.access_secret_version(
                name=f"projects/eql-data-processing/secrets/pave-agent-env-file/versions/latest"
            ).payload.data.decode("UTF-8")
            
            
            env_file.write(env_file_start)
            all_user_data = []
            
            logging.debug(f"Adding {len(user_ids)} users to docker env...\n")
            for user_id in tqdm(user_ids):
                logging.debug("Getting token for user {}".format(user_id))

                # TODO: User could have multiple access tokens (i've seen it), probably loop through all and see which one works
                rows = conn.execute(f"SELECT DISTINCT access_token FROM public.plaid_links WHERE user_id = '{user_id}'").fetchall()
                access_tokens = [ decrypt(str(row[0])) for row in rows]
                
                # If we get here we assume that user at user_id is a new user with transactions in the backend
                # we used to pull directly from the db but now we can leverage the pave-agent docker image currently running
                for access_token in access_tokens:
                    all_user_data.append((user_id, access_token))

            env_file.write("PAVE_AGENT_PAVE_USER_IDS=" + ",".join([x[0] for x in all_user_data]) + "\n")
            env_file.write("PAVE_AGENT_PLAID_ACCESS_TOKENS=" + ",".join([x[1] for x in all_user_data]) + "\n") 
    except Exception as e:
        logging.exception(e)

    # Close db connections
    cm.close_pymongo_connection()
    cm.close_postgres_connection(conn)
    
    pave_agent_start = datetime.datetime.now()
    pave_agent_run = subprocess.run(["bash","/home/langston/pave-prism/pave-agent/one-time-run-docker-cmd.sh"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    pave_agent_end = datetime.datetime.now()
    
    logging.info(f"Pave agent run took: {pave_agent_end-pave_agent_start}")
    
    with open('/home/langston/pave-prism/pave-agent-res.log', mode='w') as pave_res_file:
        pave_res_file.write(pave_agent_run.stdout.decode('utf-8'))

    process_end = datetime.datetime.now()
    logging.info(f"\nTotal runtime: {process_end-process_start}")

    return True
