import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))
from helpers import get_pg_connection


def create_views(**kwargs):
    sql_path = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "sql", "create_views.sql")
    with open(sql_path) as f:
        sql = f.read()
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("Analytical views created successfully")
    finally:
        conn.close()


with DAG(
    dag_id="build_analytical_views",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["analytics", "views"],
) as dag:

    PythonOperator(task_id="create_views", python_callable=create_views)
