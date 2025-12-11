from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

filepath = 'data/input.csv'

def decide_branch():
    if os.path.getsize(filepath) == 0:
        return 'empty_file_task'
    else:
        return 'not_empty_file_task'
    
def replace():
    df = pd.read_csv(filepath)
    df.fillna('-', inplace=True)
    df.to_csv(filepath, index=False)

with DAG(
    dag_id="airflow_task",
    schedule=None,
    default_args=default_args,
    start_date=datetime(2025, 12, 10)
) as dag:

    sensor_task = FileSensor(
        task_id = 'sensor',
        filepath= filepath,
        poke_interval= 30
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=decide_branch
    )

    empty_file_task = BashOperator(
        task_id='empty',
        bash_command="echo 'This file is empty'"
    )

    with TaskGroup(group_id="not_empty") as not_empty:
        replace_task = PythonOperator(
            task_id="replace",
            python_callable=replace
        )

    sensor_task >> branch_task >> [empty_file_task, not_empty]