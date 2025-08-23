# Установка и настройка MinIO с Docker

Данная инструкция позволяет развернуть MinIO — S3-совместимое хранилище — локально с помощью Docker Compose. Подходит для интеграции с Apache Airflow и других data-инструментов.

---

## Шаг 1: Создайте файл docker-compose.yml

Создайте новый файл с именем `docker-compose.yml` и вставьте в него следующее содержимое:

```yaml
# docker-compose.yml
version: '3.8'

services:
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
```

## Шаг 2: Запустите MinIO
Откройте терминал в папке с файлом docker-compose.yml и выполните:
```
docker-compose up -d
```

## Шаг 3: Откройте веб-интерфейс
Перейдите в браузере по адресу: http://localhost:9001

Войдите с учетными данными:


__Логин__: minioadmin

__Пароль__: minioadmin

## Шаг 4: Создайте bucket и добавьте данные
В интерфейсе нажмите Create Bucket.
Введите имя, например: airflow-test.
Нажмите Save.

Создайте файл __example.txt__, например, со следующим содержимым

```
Hello world!
```

И загрузите файл через интерфейс.

## Шаг 5: Установка необходимых библиотек

Необходимо установить дополнительный модуль находясь в WSL в окружении Airflow (помните, на [первом занятии](https://github.com/MaratNotes/marat_notes/tree/master/how_data_works-practice_cases/airflow_wsl) - 
`source ENV/bin/activate`)

Далее устанавливаем

```
pip install apache-airflow-providers-amazon
```

Этот пакет — расширение для Airflow, которое добавляет поддержку Amazon Web Services (AWS), включая S3.

Далее запускаем `airflow webserver` и `airflow scheduler` в отдельных терминалах для работы airflow.

## Шаг 6: Настройка подключения к MinIO в Airflow

Чтобы Airflow мог взаимодействовать с MinIO, необходимо создать подключение через веб-интерфейс.

### 1. Откройте Airflow UI
Перейдите в браузере по адресу:  
http://localhost:8080

### 2. Перейдите в раздел Connections
- В верхнем меню выберите **Admin → Connections**
- Нажмите кнопку **+** для добавления нового подключения

### 3. Заполните параметры подключения

| Поле | Значение |
|------|--------|
| **Conn Id** | `minio_default` |
| **Conn Type** | `Amazon Web Services` |
| **Login** | `minioadmin` |
| **Password** | `minioadmin` |
| **Host** | `http://localhost:9000` |

В поле **Extra** укажите JSON:
```json
{
  "endpoint_url": "http://localhost:9000",
}
'''

## Поздравляю! Настройка закончена
