from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
import json
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_messages():
    """Генерирует тестовые сообщения для отправки в Kafka."""
    # Генерируем случайные данные
    events = []
    for i in range(5):  # Генерируем 5 случайных событий
        user_id = f"user_{random.randint(1000, 9999)}"
        product_id = random.randint(1, 10)
        event_type = random.choice(["user_registration", "purchase"])
        
        if event_type == "user_registration":
            event_data = {
                "event_type": event_type,
                "user_id": user_id,
                "timestamp": datetime.now().isoformat(),
                "data": {"plan": random.choice(["basic", "premium", "enterprise"])}
            }
        else:  # purchase
            event_data = {
                "event_type": event_type,
                "user_id": user_id,
                "timestamp": datetime.now().isoformat(),
                "data": {
                    "product_id": product_id,
                    "amount": round(random.uniform(10.0, 500.0), 2),
                    "quantity": random.randint(1, 5)
                }
            }
        
        events.append(event_data)
    
    for record in events:
        # Ключом делаем user_id для гарантии порядка событий пользователя
        yield (str(record['user_id']).encode('utf-8'), json.dumps(record).encode('utf-8'))

with DAG(
    'kafka_producer_dag',
    default_args=default_args,
    description='DAG для отправки сообщений в Kafka',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    tags=['kafka', 'producer'],
) as dag:

    produce_task = ProduceToTopicOperator(
        task_id='produce_to_user_events',
        kafka_config_id='kafka_default',
        topic='user_events',
        producer_function=generate_messages,
    )