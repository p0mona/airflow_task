from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from settings import DEFAULT_ARGS, COPY_FILE, DS
from datetime import datetime
import os
import pandas as pd
import hashlib

def generate_hash(row) -> str:
    content = str(row.get('content', ''))
    timestamp = str(row.get('at', ''))

    data_form = f"{content}|{timestamp}"
    return hashlib.sha256(data_form.encode('utf-8')).hexdigest()

def add_hash(df) -> pd.DataFrame:
    df['hash'] = df.apply(generate_hash, axis=1)
    return df

def load_to_mongo() -> None:
    from pymongo import MongoClient, UpdateOne
    
    conn = BaseHook.get_connection('mongo_default')
    
    uri = f"mongodb://{conn.login}:{conn.password}@{conn.host}:{conn.port}/?authSource=admin"

    try:
        df = pd.read_csv(COPY_FILE)

        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        
        db = client.get_database('airflow_db')

        df = add_hash(df)

        collection = db.get_collection('mycollection')
        
        if not df.empty:
            data = df.fillna("-").to_dict('records')
            operations = []
            for record in data:
                operations.append(
                    UpdateOne(
                        {'hash': record['hash']},
                        {'$set': record},
                        upsert=True
                    )
                )
            
            collection.bulk_write(operations, ordered=False)
            print(f"Successfully loaded {len(df)} rows.")
            os.remove(COPY_FILE)
        else:
            print("No data available for loading.")
    except Exception as e:
        raise AirflowException(f"Error loading: {str(e)}")
    finally:
        if client:
            client.close()

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