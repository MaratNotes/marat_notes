from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'retreis': 5,
    'retry_delay': timedelta(minutes=2)
}

def process_daily_report(**context):
    execution_date = context['execution_date']
    print(f"!!! Обрабатываем данные за дату {execution_date}")

with DAG(
    dag_id='backfill_dag_v1',
    default_args=default_args,
    description="This is our backfill_dag",
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:
    task1 = PythonOperator(
        task_id = 'process_report',
        python_callable=process_daily_report
    )
    
    task1 