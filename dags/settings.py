from datetime import timedelta
from airflow.datasets import Dataset

DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

INPUT_PATH = 'data/'
COPY_FILE = 'data/copy.csv'

DS = Dataset(f"file://{COPY_FILE}")
ARCHIVE_FORMAT = '.zip'