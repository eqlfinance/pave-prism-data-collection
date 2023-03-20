import json

import pymongo
import sqlalchemy

from google.cloud.sql.connector import Connector
from google.cloud import secretmanager
from google.oauth2 import service_account

# TODO: This class is not sophisticated enough to handle many connections at once,
# Needs a better sence of pools and connection lifetimes
class Connection_Manager:
    def __init__(self):
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
            connector = Connector(credentials=g_credentials)
            conn = connector.connect(
                instance_connection_name,
                "pg8000",
                user=db_params_obj["DB_USER"],
                password=db_params_obj["DB_PASS"],
                db=db_params_obj["TABLE"],
            )

            return conn

        self.postgres_pool = sqlalchemy.create_engine(
            "postgresql+pg8000://", creator=get_psql_connection
        )

        mongodb_uri = client.access_secret_version(
            name=f"projects/eql-data-processing-stage/secrets/mongodb-uri/versions/latest"
        ).payload.data.decode("UTF-8")

        self.mongo_client = pymongo.MongoClient(mongodb_uri)

    def get_postgres_connection(self):
        return self.postgres_pool.connect()

    def close_postgres_connection(self, connection_obj: sqlalchemy.engine.Connection):
        connection_obj.close()

    def get_pymongo_table(self, table_name: str):
        return self.mongo_client[table_name]

    def close_pymongo_connection(self):
        self.mongo_client.close()
