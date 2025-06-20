# Airflow Wsl

## Установка Ubuntu
__Ubuntu__ — это дистрибутив Linux, в данном случае мы используем его для запуска _Apache Airflow_.

Из __PowerShell__ запустим команду

Посмотрим какие версии доступны 

```console
wsl --list --online
```

Выберем конкретную и установим c удобным для нас именем:

```console
wsl --install -d Ubuntu-24.04
```

Запустим необходимый нам экземпляр

```console
wsl -d Ubuntu-24.04
```


## Настройка Ubuntu

Обновим список доступных пакетов и их версий из репозиториев
```console
sudo apt-get update
```

Устанавим обновления для уже установленных пакетов
```console
sudo apt-get upgrade
```

Установим python
```console
sudo apt-get install python3.12-venv
```

## Установка Airflow
Создадим в домашней папке пользователя папку с настройками для Airflow
```console
mkdir airflow-setup
```

Зайдем в нее
```console
cd airflow-setup
```

Создадим виртуальное окружение, для изоляции зависимости библиотек
```console
python3 -m venv ENV
```

Активируем виртуальное окружение
```console
source ENV/bin/activate
```

Обновим pip

```console
pip install --upgrade pip
```

Установим airflow
```console
AIRFLOW_VERSION=2.11.0 && PYTHON_VERSION="$(python --version 2>&1 | cut -d ' ' -f2 | cut -d '.' -f1-2)" && CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"  && pip install "apache-airflow[async,postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

Для запуска Airflow выполним ряд команд
```console
airflow db init
```

```console
airflow webserver
```

После выпонения шагов ранее, откроем новый экземпляр Ubuntu (все еще оставляя вкладку запущенного веб-сервера).
Снова активируем виртуальную среду. Запустим команду
```console
airflow scheduler 
```

Откройте новый экземпляр Ubuntu, чтобы создать имя пользователя и пароль (не закрывайте перед этим ни одну вкладку). 
Снова активируйте виртуальную среду. !! в разделе «role» используйте только «Admin»
```console
airflow users create \
  --username local_admin \
  --firstname admin \
  --lastname local \
  --role Admin \
  --email example_email
```