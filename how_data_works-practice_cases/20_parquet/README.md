# Parquet против CSV: почему формат решает всё

Видео из цикла «Как работают данные. Apacche Spark» — разбираем, как колоночное хранение и партиционирование ускоряют аналитику в десятки и сотни раз.

[Telegram](https://t.me/marat_notes)
[Youtube](https://youtu.be/RqWQpgwQuWE)
[Vk](https://vkvideo.ru/video-231048746_456239043)

## Структура_прроекта

├── Apache_Spark-Parquet.pdf # Презентация (13 слайдов)
├── generate_data.py # Генерация 10 млн строк в CSV / Parquet
├── benchmark_parquet.py # Бенчмарк: чтение, фильтрация, партиционирование
└── README.md


## Быстрый старт

```bash
# Установка зависимостей
pip install pandas pyarrow numpy

# 1. Генерация данных (1 раз, ~2 минуты)
python generate_data.py

# 2. Запуск бенчмарка (мгновенно)
python benchmark_parquet.py