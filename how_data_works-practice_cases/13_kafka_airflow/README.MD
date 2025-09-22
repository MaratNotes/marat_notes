## Airflow и Kafka: интеграция для надежной обработки данных

## Архитектура решения
Данный проект демонстрирует интеграцию Apache Airflow и Apache Kafka для построения отказоустойчивых ETL-пайплайнов. 

Решение состоит из двух основных компонентов:

Producer DAG - генерирует тестовые события и отправляет их в Kafka

Consumer DAG - читает события из Kafka, обрабатывает их и сохраняет результаты


### 1) Рассмотрим основные настройки в docker-compose-файле
```
kafka:
  image: confluentinc/cp-kafka:7.3.0
  ports:
    - "9092:9092"      # Внутри контейнеров
    - "29092:29092"    # Снаружи (ваша машина)
  environment:
    KAFKA_BROKER_ID: 1 
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181 
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT 
    KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

_KAFKA_BROKER_ID_: 1

Уникальный идентификатор брокера. В продакшене — несколько брокеров с ID 1, 2, 3…
Здесь — один, поэтому 1. 

_KAFKA_ZOOKEEPER_CONNECT_: zookeeper:2181

Где развернут zookeeper

_KAFKA_LISTENER_SECURITY_PROTOCOL_MAP_: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT

Определяет, какие протоколы используются для входящих соединений.
PLAINTEXT — без шифрования. Используем, потому что это локальная среда. 

_KAFKA_INTER_BROKER_LISTENER_NAME_: PLAINTEXT

Как Kafka общается между брокерами.
У нас один брокер, поэтому PLAINTEXT — достаточно. 

_KAFKA_ADVERTISED_LISTENERS_ — САМАЯ ВАЖНАЯ НАСТРОЙКА

Говорит клиентам: «Как к вам подключиться?» 


PLAINTEXT://kafka:9092 — для сервисов внутри Docker (Airflow, scheduler)
PLAINTEXT_HOST://localhost:29092 — для внешних клиентов (ваш браузер, Kafka UI)
Без этого — Airflow не подключится к Kafka.


_KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR_: 1

Сколько копий хранить для служебного топика __consumer_offsets.

_KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR_: 1

Копии для топика транзакций.

KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

Минимальное число реплик, которые должны быть «в синхроне», чтобы Kafka могла писать.

KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

Kafka сама создаёт топики, если такой не создан


### 2) Запустите docker-compose
```
docker-compose up -d
```


### 3) Добавьте в connection следующую настройку

Это необходимо, так как 

В настройках Kafka у нас есть:

_KAFKA_ADVERTISED_LISTENERS_: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092

Это значит, что внутри Docker-сети Kafka доступна по адресу kafka:9092, а с хоста по localhost:29092.

Так как Airflow и Kafka взаимодействуют внутри Docker-сети, то мы должны использовать kafka:9092

### 4) Запустите даги из папки  dags - сначала producer, потом consumer

Consumer читает уже существующие сообщения.
Если запустить Consumer первым — он будет ждать новых событий, а их ещё нет.
Запуск Producer → ждём → запуск Consumer — гарантирует, что вы увидите обработку.

### 5) http://localhost:8081 - проверьте состояние топика 
Перейдите: local → Topics → user_events и посмотрите вкладки -_MESSAGES_ и _CONSUMERS_

Это пример демонстрирует использование сочетания Airflow и Kafka для построения надежных и масштабируемых систем, способных обрабатывать большие объемы событий.
