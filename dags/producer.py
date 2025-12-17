from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from settings import DEFAULT_ARGS, INPUT_PATH, COPY_FILE, DS, ARCHIVE_FORMAT
from datetime import datetime
import pandas as pd
import os
import zipfile

def input_paths() -> list:
    paths = []

    if not os.path.exists(INPUT_PATH) or not os.listdir(INPUT_PATH):
        return paths 
    
    for file in os.listdir(INPUT_PATH):
        file_path = os.path.join(INPUT_PATH, file)
        
        if os.path.isfile(file_path) and file.endswith(ARCHIVE_FORMAT):
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(INPUT_PATH)
            os.remove(file_path)
    
    for root, _, files in os.walk(INPUT_PATH):
        if '__MACOSX' in root:
            continue
            
        for file in files:
            if file.endswith('.csv'):
                paths.append(os.path.join(root, file))
    
    return paths

def combine_and_decide() -> str:
    paths = input_paths()

    if not paths:
        return 'empty'
    else: 
        target = pd.concat((pd.read_csv(path) for path in paths), ignore_index=True)
        target.to_csv(COPY_FILE, index=False)
        return 'not_empty.replace'
    
def replace() -> None:
    df = pd.read_csv(COPY_FILE)
    df.fillna('-', inplace=True)
    df.to_csv(COPY_FILE, index=False)

def sort() -> None:
    df = pd.read_csv(COPY_FILE)
    df.sort_values('at', inplace=True)
    df.to_csv(COPY_FILE, index=False)

def clean() -> None:
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
        filepath= INPUT_PATH,
        poke_interval= 30
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=combine_and_decide
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