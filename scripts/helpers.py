import os
from pymongo import MongoClient
import psycopg2


def get_mongo_client():
    uri = os.environ.get("MONGO_URI", "mongodb://root:rootpassword@localhost:27017/")
    return MongoClient(uri)


def get_mongo_db():
    client = get_mongo_client()
    db_name = os.environ.get("MONGO_DB", "etl_source")
    return client[db_name]


def get_pg_connection():
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=os.environ.get("PG_PORT", "5432"),
        dbname=os.environ.get("PG_DB", "airflow"),
        user=os.environ.get("PG_USER", "airflow"),
        password=os.environ.get("PG_PASSWORD", "airflow"),
    )
