from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="airflow_task",
    schedule=None,
    default_args=default_args,
    start_date=datetime(2025, 12, 10)
) as dag:
    sensor_task = FileSensor(
        task_id = 'sensor',
        filepath= 'data/input.csv',
        poke_interval= 30,
    )