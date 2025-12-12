from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from pymongo import MongoClient
from airflow import Dataset
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

input_file = 'data/input.csv'
copy_file = 'data/copy.csv'

ds = Dataset(f"file://data/copy.csv")

def decide_branch():
    if os.path.getsize(input_file) == 0:
        return 'empty'
    else:
        df = pd.read_csv(input_file)
        df.to_csv(copy_file, index=False)
        return 'not_empty.replace'
    
def replace():
    df = pd.read_csv(copy_file)
    df.fillna('-', inplace=True)
    df.to_csv(copy_file, index=False)

def sort():
    df = pd.read_csv(copy_file)
    df.sort_values('at', inplace=True)
    df.to_csv(copy_file, index=False)

def clean():
    df = pd.read_csv(copy_file)
    df['content'] = df['content'].str.replace(
        r"[^\w.,!?;:\-()… ]+",
        "",
        regex=True
    )
    df.to_csv(copy_file, index=False)

with DAG(
    dag_id="airflow_task",
    schedule=None,
    default_args=default_args,
    start_date=datetime(2025, 12, 10)
) as dag1:

    sensor_task = FileSensor(
        task_id = 'sensor',
        filepath= input_file,
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
            outlets=[ds]
        )

        replace_task >> sort_task >> clean_task

    sensor_task >> branch_task >> [empty_file_task, not_empty]

def load_to_mongo():
    conn = BaseHook.get_connection('mongo_default')
    
    uri = f"mongodb://{conn.login}:{conn.password}@{conn.host}:{conn.port}/?authSource=admin"
    df = pd.read_csv(copy_file)
    
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client.get_database('airflow_db')
    collection = db.get_collection('mycollection')
    
    if not df.empty:
        data = df.fillna("-").to_dict('records')
        collection.delete_many({})
        collection.insert_many(data)
        print(f"Successfully loaded {len(df)} rows.")
    else:
        print("No data available for loading.")

with DAG(
    dag_id="airflow_mongo_task",
    schedule=[ds],
    default_args=default_args,
    start_date=datetime(2025, 12, 10)
) as dag2:
    load_data = PythonOperator(
        task_id='load_to_mongo',
        python_callable=load_to_mongo,
    )

    load_data