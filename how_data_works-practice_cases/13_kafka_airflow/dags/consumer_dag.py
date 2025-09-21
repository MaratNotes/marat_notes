from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.python import PythonOperator
import json
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_message(message):
    """Обрабатывает одно сообщение из Kafka."""
    logger = logging.getLogger(__name__)
    
    try:
        # Декодируем ключ и значение сообщения
        message_key = message.key().decode('utf-8') if message.key() else None
        message_value = message.value().decode('utf-8')
        event_data = json.loads(message_value)
        
        logger.info(f"📥 Получено сообщение. Ключ: {message_key}, Данные: {event_data}")
        
        # Бизнес-логика обработки разных типов событий
        if event_data['event_type'] == 'user_registration':
            logger.info(f"Новый пользователь: {event_data['user_id']} с тарифом {event_data['data']['plan']}")
        elif event_data['event_type'] == 'purchase':
            logger.info(f"Покупка от {event_data['user_id']}: продукт {event_data['data']['product_id']}, сумма {event_data['data']['amount']}")
        
        return event_data
        
    except Exception as e:
        logger.error(f"!!! Ошибка обработки сообщения: {e}")
        return None

def save_processed_data(**context):
    """Сохраняет обработанные данные (пример)."""
    logger = logging.getLogger(__name__)
    
    # Получаем все обработанные сообщения из XCom
    ti = context['ti']
    messages = ti.xcom_pull(task_ids='consume_from_user_events', key='return_value')
    
    if messages:
        # Фильтруем None (ошибки обработки)
        successful_messages = [msg for msg in messages if msg is not None]
        logger.info(f"Сохранение {len(successful_messages)} обработанных событий")
        
        # Здесь может быть запись в БД, S3 и т.д.
        for record in successful_messages:
            logger.info(f"Сохранение: {record['user_id']} - {record['event_type']}")
        
        return f"Успешно сохранено {len(successful_messages)} записей"
    else:
        logger.info("Нет данных для сохранения")
        return "Нет данных для сохранения"

with DAG(
    'kafka_consumer_dag',
    default_args=default_args,
    description='DAG для чтения и обработки сообщений из Kafka',
    schedule_interval=timedelta(minutes=35),
    catchup=False,
    tags=['kafka', 'consumer'],
) as dag:

    consume_task = ConsumeFromTopicOperator(
        task_id='consume_from_user_events',
        kafka_config_id='kafka_default',
        topics=['user_events'],
        apply_function=process_message,
        commit_cadence='end_of_batch',
        max_messages=50,
        max_batch_size=10,
        consumer_config={
            'auto.offset.reset': 'earliest',
            'group.id': 'airflow-consumer-group',
            'enable.auto.commit': False,
        },
    )

    save_task = PythonOperator(
        task_id='save_processed_data',
        python_callable=save_processed_data,
        provide_context=True,
    )

    consume_task >> save_task