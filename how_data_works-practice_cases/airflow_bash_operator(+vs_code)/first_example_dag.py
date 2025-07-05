from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='first_example_dag',
    default_args=default_args,
    description='This is first dag!',
    start_date=datetime(2025, 7, 4),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo It is first task!'
    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo It is second task!'
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo It is third task!'
    )

    task4 = BashOperator(
        task_id='fourth_task',
        bash_command='echo It is fourth task!'
    )

    task1 >> [task2, task3] >> task4