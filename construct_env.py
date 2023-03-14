import base64
import datetime
import json
import logging
import subprocess

from typing import List
from cryptography.fernet import Fernet, MultiFernet
from tqdm import tqdm

from db_connections import Connection_Manager

from google.cloud import secretmanager

secret_manager_client = secretmanager.SecretManagerServiceClient()

keys = secret_manager_client.access_secret_version(
    name=f"projects/eql-data-processing-stage/secrets/pave-agent-decryption-keys/versions/latest"
).payload.data.decode("UTF-8")
keys = json.loads(keys)["KEYS"]


def base64_decode(val: str) -> bytes:
    return base64.urlsafe_b64decode(val.encode("ascii"))


def decrypt(val: str) -> str:
    fernet = MultiFernet(Fernet(k) for k in keys)
    actual = fernet.decrypt(base64_decode(val))
    return actual.decode()

'''
    - collects user_ids and thier access tokens,
    - prints them to an env
    - runs the pave agent docker image
'''
def run(user_ids: List[str] = []) -> bool:

    cm = Connection_Manager()
    process_start = datetime.datetime.now()

    try:
        with open("/home/langston/pave-prism/pave-agent/one-time-run.env", mode="w") as env_file:
            # Open connection to postgres db
            conn = cm.get_postgres_connection()


            if len(user_ids) < 1:
                # Close db connection
                cm.close_postgres_connection(conn)

                process_end = datetime.datetime.now()
                logging.info(f"No new users found. Ending process")
                logging.info(f"\nTotal runtime: {process_end-process_start}")
                return True

            # Get pave secret values
            env_file_start = secret_manager_client.access_secret_version(
                name=f"projects/eql-data-processing-stage/secrets/pave-agent-env-file/versions/latest"
            ).payload.data.decode("UTF-8")

            env_file.write(env_file_start)
            all_user_data = []

            # For user in user_ids add their user_id and decrypted access token to the env file
            logging.debug(f"Adding {len(user_ids)} users to docker env...\n")
            for user_id in tqdm(user_ids):
                logging.debug("Getting token for user {}".format(user_id))

                rows = conn.execute(
                    f"SELECT DISTINCT access_token FROM public.plaid_links WHERE user_id = '{user_id}'"
                ).fetchall()
                access_tokens = [decrypt(str(row[0])) for row in rows]

                for access_token in access_tokens:
                    all_user_data.append((user_id, access_token))

            # Write to env
            env_file.write(
                "PAVE_AGENT_PAVE_USER_IDS="
                + ",".join([x[0] for x in all_user_data])
                + "\n"
            )
            env_file.write(
                "PAVE_AGENT_PLAID_ACCESS_TOKENS="
                + ",".join([x[1] for x in all_user_data])
                + "\n"
            )
    except Exception as e:
        logging.exception(e)

    # Close db connections
    cm.close_pymongo_connection()
    cm.close_postgres_connection(conn)

    pave_agent_start = datetime.datetime.now()
    
    # Call a docker run script that does a one-time pave agent data collection on all users in user_ids
    pave_agent_run = subprocess.run(
        ["bash", "/home/langston/pave-prism/pave-agent/one-time-run-docker-cmd.sh"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    pave_agent_end = datetime.datetime.now()
    logging.info(f"Pave agent run took: {pave_agent_end-pave_agent_start}")

    # Write the pave agent logs to my home folder for easy access
    with open("/home/langston/pave-prism/pave-agent-res.log", mode="w") as pave_res_file:
        pave_res_file.write(pave_agent_run.stdout.decode("utf-8"))

    process_end = datetime.datetime.now()
    logging.info(f"\nTotal runtime: {process_end-process_start}")

    return True
