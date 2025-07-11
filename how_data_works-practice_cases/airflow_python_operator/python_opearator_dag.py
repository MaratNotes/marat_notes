from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'retreis': 5,
    'retry_delay': timedelta(minutes=2)
}

def hello(add_str: str, add_number: int):
    print(f"Hello, i am python operator! Add string is {add_str}, add_numer is {add_number}")

with DAG(
    dag_id='python_operator_dag_v2',
    default_args=default_args,
    description="This is our python_operator v2",
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id = 'python_task',
        python_callable=hello,
        op_kwargs={'add_str': "fine", 'add_number': 5},    
    )
    
    task1 