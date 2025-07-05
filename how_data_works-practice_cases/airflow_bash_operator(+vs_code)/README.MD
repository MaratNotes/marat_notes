# Airflow Bash Operator (+ VS_Code)

## VSCode

### Установка VSCode

### Основные настройки VSCode

Обязательные расширения:

- Remote - WSL (для работы с файлами в WSL)
- Python (поддержка Python/DAGs)
- Docker (если используете контейнеры)

### Настройка проекта:

Откройте папку **WSL** в **VSCode**:
*Ctrl+Shift+P* → *Remote Explorer: Focus on WSL Targets View* → */home/ваш_пользователь/airflow*

Создадим папку **dags**, и у проекта будет следующая структура (создадим папку **dags**, если её не существует):

├── dags/              # Ваши DAG-файлы

└── airflow.cfg        # Конфиг (если кастомизирован)

Также добавим в качестве интерпретатора python из Venv:
*Ctrl+Shift+P* → *Python: Select Interpretator* → */home/local_admin/airflow-setup/ENV/bin/python3.12*


### Интеграция Airflow с WSL
В airflow.cfg установите:

Автоматическая перезагрузка DAGs:
```
dag_dir_list_interval = 30  # Проверка новых DAG каждые 30 сек
```
Также отключим переменную отвечающую за export 
```
AIRFLOW__CORE__LOAD_EXAMPLES=False
```