import datetime
from airflow import DAG
# Импортируем SQLExecuteQueryOperator вместо PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Определяем аргументы DAG'а по умолчанию
default_args = {
    'owner': 'finance_team',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Создаем DAG
with DAG(
    dag_id='finance_postgres_dag',
    default_args=default_args,
    description='DAG для работы с таблицей клиентов в PostgreSQL (с SQLExecuteQueryOperator)',
    schedule_interval='@once',
    catchup=False,
    tags=['finance', 'postgres', 'sql'],
) as dag:

    # Задача 1: Создать таблицу клиентов (используем SQLExecuteQueryOperator)
    create_client_table = SQLExecuteQueryOperator(
        task_id='create_client_table',
        conn_id='postgres_default', 
        sql='sql/create_client_table.sql',
    )

    # Задача 2: Вставить данные клиентов
    populate_client_table = SQLExecuteQueryOperator(
        task_id='populate_client_table',
        conn_id='postgres_default',
        sql='sql/insert_clients.sql',
    )

    # Задача 3: Выбрать всех клиентов (простой SELECT)
    get_all_clients = SQLExecuteQueryOperator(
        task_id='get_all_clients',
        conn_id='postgres_default',
        sql='SELECT * FROM clients;',
    )

    # Определяем зависимости между задачами
    create_client_table >> populate_client_table >> get_all_clients

with DAG(
    dag_id='insert_finance_postgres_dag_v2',
    default_args=default_args,
    description='DAG для вставки данных в таблицу клиентов в PostgreSQL (с SQLExecuteQueryOperator)',
    schedule_interval='@once',
    catchup=False,
    tags=['finance', 'postgres', 'sql'],
) as dag:

    
    insert_single_client = SQLExecuteQueryOperator(
        task_id='insert_single_client',
        conn_id='postgres_default', 
        sql='sql/insert_example.sql',
    parameters={
        'client_name': "Петр Петров", 
        'client_email': "petr@mail.ru",
        'client_phone': "+79999988778",
        'client_address': "г. Иваново, Ивановская 2, дом 2"
        },
    )
    

    insert_single_client