# Airflow + MinIO: ETL-пайплайн с выгрузкой данных за день

Этот проект демонстрирует пайплайн:
1. Генерация данных о клиентах
2. Ручная загрузка в PostgreSQL
3. Выгрузка данных за каждый день в CSV
4. Загрузка в MinIO (S3-совместимое хранилище)

---

## Шаг 1: Создайте файл docker-compose.yml

Создайте новый файл с именем `docker-compose.yml` и вставьте в него следующее содержимое:

```yaml
# docker-compose.yml
version: '3.8'

services:
  postgres:
    image: postgres:15
    container_name: airflow_postgres_example
    environment:
      POSTGRES_USER: example_user
      POSTGRES_PASSWORD: example_pass
      POSTGRES_DB: example_db
    ports:
      - "5532:5432"
    volumes:
      - postgres_data_volume:/var/lib/postgresql/data 

  minio:
    image: quay.io/minio/minio
    container_name: minio
    ports:
      - "9000:9000"  # S3 API
      - "9001:9001"  # Web Console
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio_data:/data
    command: server /data --console-address ":9001"
    restart: unless-stopped

volumes:
  minio_data:
  postgres_data_volume:
 ```
 
 ## Шаг 2: Запуск образа
 
 ```
 docker-compose up -d
 ```
 
 ## Шаг 3 Генерация данных
 
Испольуя файл в репозитории __generate_clients.py__ сгенерировать клиентов. Результат - __clients_data.csv__

## Шаг 4 Создание таблицы

Звпустите скрипт для создания таблицы

```
CREATE TABLE clients (
    id INTEGER PRIMARY KEY,
    surname VARCHAR(50),
    name VARCHAR(50),
    patronymic VARCHAR(50),
    city VARCHAR(50),
    entry_date DATE
);
```

## Шаг 5 Импорт данных
1. В DBeaver кликните правой кнопкой мыши по таблице `clients` → **Import Data**.
2. Выберите:
   - **Format**: CSV
   - **File**: Укажите путь к файлу `clients_data.csv` (например, `/home/user/clients_data.csv`)
3. На вкладке **Settings**:
   - Разделитель: `,`
   - Кодировка: `UTF-8`
   - Отметьте: ✅ **First line contains column names (headers)**
4. На вкладке **Mapping**:
   - Убедитесь, что все столбцы сопоставлены корректно (id → id, surname → surname и т.д.)
5. Нажмите **Finish**.

## Поздравляю! Подготовка для создания ETL завершена!