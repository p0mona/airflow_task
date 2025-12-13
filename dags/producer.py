from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from settings import DEFAULT_ARGS, INPUT_FILE, COPY_FILE, DS
from datetime import datetime
import pandas as pd
import os

def decide_branch():
    if os.path.getsize(INPUT_FILE) == 0:
        return 'empty'
    else:
        df = pd.read_csv(INPUT_FILE)
        df.to_csv(COPY_FILE, index=False)
        return 'not_empty.replace'
    
def replace():
    df = pd.read_csv(COPY_FILE)
    df.fillna('-', inplace=True)
    df.to_csv(COPY_FILE, index=False)

def sort():
    df = pd.read_csv(COPY_FILE)
    df.sort_values('at', inplace=True)
    df.to_csv(COPY_FILE, index=False)

def clean():
    df = pd.read_csv(COPY_FILE)
    df['content'] = df['content'].str.replace(
        r"[^\w.,!?;:\-()… ]+",
        "",
        regex=True
    )
    df.to_csv(COPY_FILE, index=False)

with DAG(
    dag_id="producer",
    schedule=None,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 12, 10)
) as dag1:

    sensor_task = FileSensor(
        task_id = 'sensor',
        filepath= INPUT_FILE,
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

        sort_task = PythonOperator(
            task_id="sort",
            python_callable=sort
        )

        clean_task = PythonOperator(
            task_id="clean",
            python_callable=clean,
            outlets=[DS]
        )

        replace_task >> sort_task >> clean_task

    sensor_task >> branch_task >> [empty_file_task, not_empty]

