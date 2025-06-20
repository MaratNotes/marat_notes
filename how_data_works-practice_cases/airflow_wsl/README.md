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

Установим пакетный менеджер
```console
sudo apt-get install npm
```

Установим python
```console
sudo apt-get install python3.12-venv
```
