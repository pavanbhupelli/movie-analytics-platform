from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

default_args = {
    "owner": "pavan",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

dag = DAG(
    dag_id="movie_analytics_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# ---------------- TASKS ----------------

def bronze():
    subprocess.run(["python", "/opt/airflow/etl/bronze_reader.py"], check=True)

def silver():
    subprocess.run(["python", "/opt/airflow/etl/silver_transform.py"], check=True)

def gold():
    subprocess.run(["python", "/opt/airflow/etl/gold_transform.py"], check=True)

def snowflake_load():
    subprocess.run(["python", "/opt/airflow/etl/snowflake_load.py"], check=True)

bronze_task = PythonOperator(
    task_id="bronze_layer",
    python_callable=bronze,
    dag=dag
)

silver_task = PythonOperator(
    task_id="silver_layer",
    python_callable=silver,
    dag=dag
)

gold_task = PythonOperator(
    task_id="gold_layer",
    python_callable=gold,
    dag=dag
)

snowflake_task = PythonOperator(
    task_id="snowflake_load",
    python_callable=snowflake_load,
    dag=dag
)

# ---------------- FLOW ----------------
bronze_task >> silver_task >> gold_task >> snowflake_task