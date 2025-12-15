from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from settings import DEFAULT_ARGS, COPY_FILE, DS
from datetime import datetime
import pandas as pd

def load_to_mongo() -> None:
    from pymongo import MongoClient
    
    conn = BaseHook.get_connection('mongo_default')
    
    uri = f"mongodb://{conn.login}:{conn.password}@{conn.host}:{conn.port}/?authSource=admin"
    df = pd.read_csv(COPY_FILE)
    
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
    dag_id="consumer",
    schedule=[DS],
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 12, 10)
) as dag2:
    load_data = PythonOperator(
        task_id='load_to_mongo',
        python_callable=load_to_mongo,
    )

    load_data