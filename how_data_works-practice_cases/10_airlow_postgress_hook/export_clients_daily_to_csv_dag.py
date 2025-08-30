# dags/export_clients_daily_to_csv.py
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import tempfile
import os

# --- Глобальные параметры ---
S3_BUCKET = 'airflow-bucket'
S3_KEY_PREFIX = 'daily_export/clients_'  # будет: daily_export/clients_2025-05-20.csv
# ----------------------------

def extract_and_save_to_csv(**context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    execution_date = context['ds']
    print(f"Обработка данных за дату: {execution_date}")

    sql = """
        SELECT id, surname, name, patronymic, city, entry_date
        FROM clients
        WHERE entry_date = %s
    """

    df = hook.get_pandas_df(sql, parameters=[execution_date])

    if df.empty:
        print(f"Нет данных для даты {execution_date}")
        context['task_instance'].xcom_push(key='file_skipped', value=True)
        return

    print(f"Найдено {len(df)} записей. Сохраняем во временный файл...")

    # Создаём временный файл
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as tmp_file:
        df.to_csv(tmp_file.name, index=False)
        tmp_file_path = tmp_file.name
        print(f"Файл временно сохранён: {tmp_file_path}")

    # Передаём путь к временному файлу в XCom
    context['task_instance'].xcom_push(key='csv_file_path', value=tmp_file_path)


def upload_to_minio(**context):
    tmp_file_path = context['task_instance'].xcom_pull(task_ids='extract_and_save_csv', key='csv_file_path')
    execution_date = context['ds']

    if not tmp_file_path:
        print(f"Файл для даты {execution_date} не был создан. Пропуск загрузки.")
        return

    s3_key = f"{S3_KEY_PREFIX}{execution_date}.csv"

    hook = S3Hook(aws_conn_id='minio_default')
    hook.load_file(
        filename=tmp_file_path,
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True
    )
    print(f"Файл {tmp_file_path} успешно загружен в s3://{S3_BUCKET}/{s3_key}")

    # Удаляем временный файл после загрузки
    if os.path.exists(tmp_file_path):
        os.remove(tmp_file_path)
        print(f"Временный файл удалён: {tmp_file_path}")


# === Динамический start_date: 5 дней назад от сегодня ===
TODAY = datetime.now().date()
START_DATE = TODAY - timedelta(days=5)
dag_start_date = datetime.combine(START_DATE, datetime.min.time())
# =======================================================

with DAG(
    dag_id="export_clients_daily_to_csv",
    description="Выгрузка данных о клиентах за текущую дату в CSV и загрузка в MinIO",
    start_date=dag_start_date,
    schedule_interval="@daily",
    catchup=True,
    tags=["etl", "postgres", "csv", "s3", "clients"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    extract_task = PythonOperator(
        task_id="extract_and_save_csv",
        python_callable=extract_and_save_to_csv,
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio,
        provide_context=True,
    )

    extract_task >> upload_task