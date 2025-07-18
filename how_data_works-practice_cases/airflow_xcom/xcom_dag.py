from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.taskinstance import TaskInstance

default_args = {
    'retreis': 5,
    'retry_delay': timedelta(minutes=2)
}

def hello(**context):
    ti: TaskInstance = context["ti"]
    xcom_str1 = ti.xcom_pull(task_ids='get_hello_str_xcom', key='xcom_str1')
    xcom_str2 = ti.xcom_pull(task_ids='get_hello_str_xcom', key='xcom_str2')
    xcom_number = ti.xcom_pull(task_ids='get_number_xcom', key='xcom_number')
    print(f"{xcom_str1} {xcom_str2}, add_numer is {xcom_number}")

def get_hello_str_xcom(**context):
    ti: TaskInstance = context["ti"]
    ti.xcom_push(key="xcom_str1", value = "Hello, i am xcom string!")
    ti.xcom_push(key="xcom_str2", value = "I am another xcom string!")

def get_number_xcom(**context):
    ti: TaskInstance = context["ti"]
    ti.xcom_push(key="xcom_number", value = 21)
    

with DAG(
    dag_id='xcom_dag_v02',
    default_args=default_args,
    description="This is x-com example",
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id = 'python_task',
        python_callable=hello,
        op_kwargs={'add_number': 5},    
    )

    task2 = PythonOperator(
        task_id = 'get_hello_str_xcom',
        python_callable=get_hello_str_xcom,  
    )

    task3 = PythonOperator(
        task_id = 'get_number_xcom',
        python_callable=get_number_xcom,  
    )
    
    [task3, task2] >> task1 