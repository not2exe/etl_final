import json
import sys
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))
from helpers import get_mongo_db, get_pg_connection


def generate_data(**kwargs):
    from generate_data import main
    main()


def create_tables(**kwargs):
    sql_path = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "sql", "create_tables.sql")
    with open(sql_path) as f:
        sql = f.read()
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Tables created successfully")
    finally:
        conn.close()


def _replicate_collection(collection_name, table_name, columns, transform_row):
    from psycopg2.extras import execute_values

    db = get_mongo_db()
    docs = list(db[collection_name].find({}, {"_id": 0}))
    print(f"Fetched {len(docs)} documents from {collection_name}")

    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table_name}")
            if docs:
                rows = [transform_row(doc) for doc in docs]
                placeholders = ", ".join(["%s"] * len(columns))
                query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
                template = f"({placeholders})"
                execute_values(cur, query, rows, template=template)
        conn.commit()
        print(f"Replicated {len(docs)} rows into {table_name}")
    finally:
        conn.close()


def replicate_user_sessions(**kwargs):
    def transform(doc):
        return (
            doc["session_id"],
            doc["user_id"],
            doc["start_time"],
            doc["end_time"],
            json.dumps(doc["pages_visited"]),
            json.dumps(doc["device"]),
            json.dumps(doc["actions"]),
        )
    _replicate_collection(
        "user_sessions", "user_sessions",
        ["session_id", "user_id", "start_time", "end_time", "pages_visited", "device", "actions"],
        transform,
    )


def replicate_event_logs(**kwargs):
    def transform(doc):
        return (
            doc["event_id"],
            doc["user_id"],
            doc["timestamp"],
            doc["event_type"],
            json.dumps(doc["details"]),
        )
    _replicate_collection(
        "event_logs", "event_logs",
        ["event_id", "user_id", "timestamp", "event_type", "details"],
        transform,
    )


def replicate_support_tickets(**kwargs):
    def transform(doc):
        return (
            doc["ticket_id"],
            doc["user_id"],
            doc["created_at"],
            doc["updated_at"],
            doc["status"],
            doc["issue_type"],
            json.dumps(doc["messages"]),
        )
    _replicate_collection(
        "support_tickets", "support_tickets",
        ["ticket_id", "user_id", "created_at", "updated_at", "status", "issue_type", "messages"],
        transform,
    )


def replicate_user_recommendations(**kwargs):
    def transform(doc):
        return (
            doc["user_id"],
            json.dumps(doc["recommended_products"]),
            doc["last_updated"],
        )
    _replicate_collection(
        "user_recommendations", "user_recommendations",
        ["user_id", "recommended_products", "last_updated"],
        transform,
    )


def replicate_moderation_queue(**kwargs):
    def transform(doc):
        return (
            doc["review_id"],
            doc["user_id"],
            doc["product_id"],
            doc["review_text"],
            doc["rating"],
            doc["moderation_status"],
            json.dumps(doc["flags"]),
            doc["created_at"],
        )
    _replicate_collection(
        "moderation_queue", "moderation_queue",
        ["review_id", "user_id", "product_id", "review_text", "rating", "moderation_status", "flags", "created_at"],
        transform,
    )


with DAG(
    dag_id="mongo_to_postgres_replication",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "replication"],
) as dag:

    t_generate = PythonOperator(task_id="generate_data", python_callable=generate_data)
    t_create_tables = PythonOperator(task_id="create_tables", python_callable=create_tables)

    t_rep_sessions = PythonOperator(task_id="replicate_user_sessions", python_callable=replicate_user_sessions)
    t_rep_events = PythonOperator(task_id="replicate_event_logs", python_callable=replicate_event_logs)
    t_rep_tickets = PythonOperator(task_id="replicate_support_tickets", python_callable=replicate_support_tickets)
    t_rep_recs = PythonOperator(task_id="replicate_user_recommendations", python_callable=replicate_user_recommendations)
    t_rep_moderation = PythonOperator(task_id="replicate_moderation_queue", python_callable=replicate_moderation_queue)

    t_generate >> t_create_tables >> [t_rep_sessions, t_rep_events, t_rep_tickets, t_rep_recs, t_rep_moderation]
