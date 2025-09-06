# dags/test_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_func():
    print("Hello from Docker!")

with DAG(
    'hello_dag',
    start_date=datetime(2025,1,1),
    schedule_interval=None
) as dag:
    hello_task = PythonOperator(task_id='hello_func', python_callable=hello)