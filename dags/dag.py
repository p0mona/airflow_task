from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
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

dag = DAG(
    dag_id="airflow_task",
    schedule=None,
    default_args=default_args,
    start_date=datetime(2025, 12, 10)
) 

sensor_task = FileSensor(
    task_id = 'sensor',
    filepath= filepath,
    poke_interval= 30,
    dag=dag
)

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=decide_branch,
    dag=dag
)

empty_file_task = BashOperator(
    task_id='empty_file_task',
    bash_command="echo 'This file is empty'",
    dag=dag
)

with TaskGroup(group_id="not_empty") as not_empty:
    
    ...

sensor_task >> branch_task >> [empty_file_task, not_empty]