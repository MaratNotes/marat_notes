from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# Параметры
BUCKET_NAME = 'airflow-data'
KEY = 'example.txt'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    's3_example_dag',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['sensor', 'minio', 'demo']
) as dag:

    wait_for_file = S3KeySensor(
        task_id='wait_for_trigger_file',
        bucket_key=KEY,
        bucket_name=BUCKET_NAME,
        aws_conn_id='minio_default',
        mode='poke',
        poke_interval=10,
        timeout=300,
        soft_fail=False
    )