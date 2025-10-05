from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task


default_args = {
    'retreis': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='task_flow_api_dag_v1',
    default_args=default_args,
    description="This is TaskFlowApi example",
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    @task
    def get_hello_strings() -> dict[str, str]:
        return {
            "key_str1": "Hello, i am xcom string!",
            "key_str2": "I am another xcom string!"
        }
    
    @task 
    def get_number()  -> int:
        return 21
    
    @task
    def union_example_tasks(str_data, number):
        str1 = str_data["key_str1"]
        str2 = str_data["key_str2"]
        numb = number
        
        print(f"{str1} {str2}, numb is {numb}")

    data_strings = get_hello_strings()
    number = get_number()

    union_example_tasks(data_strings, number)