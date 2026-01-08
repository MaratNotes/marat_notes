# PySpark на Windows через Docker

Минимальный пример запуска PySpark на Windows 10.

> Видео с пошаговой инструкцией: [ссылка на YouTube]

---

## Что нужно

1. **Docker Desktop**  
   Скачайте и установите: https://www.docker.com/products/docker-desktop  
   После установки **перезагрузите компьютер**, если потребуется.

2. **PowerShell** (встроен в Windows)  
   **Не используйте Git Bash** — он несовместим с путями Docker на Windows.

---

## Как запустить

1. Создайте папку проекта, например:  
   `D:\projects\pyspark-docker`

2. Откройте **PowerShell в этой папке**:  
   - В проводнике: **Shift + ПКМ** → «Открыть окно PowerShell здесь»

3. Выполните команду:
   ```powershell
   docker run -p 8888:8888 --name pyspark-jupyter -v ${PWD}:/home/jovyan/work jupyter/pyspark-notebook
   ```

4. Скопируйте ссылку с токеном из терминала и откройте в браузере.

---
## Запуск примера
Базовый пример

```
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("first-test").master("local[*]").getOrCreate()
data = [(1, "Январь"),(2, "Февраль"),(3, "Март")]
df = spark.createDataFrame(data, ["индекс месяца","название"])
df.show()
spark.stop()
```

Пример с загрузкой данных из файла. Для этого создайте файл months.csv

```
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("csv-test").master("local[*]").getOrCreate()
data = [(1, "Январь"),(2, "Февраль"),(3, "Март")]
df = spark.read.csv("months.csv", header=True)
df.show()
spark.stop()
```
---
## Важные дополнения
1. Чтобы завершить процесс в PowerShell сделайте двойное нажатие Ctrl + com/products/docker-desktop
2, Чтобы удалить процесс выполните команду docker rm pyspark-jupyter
