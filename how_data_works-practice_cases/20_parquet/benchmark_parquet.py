"""
Бенчмарк: сравнение скорости чтения и фильтрации
"""

import pandas as pd
import time
from pathlib import Path

partitioned_path = Path('data_partitioned')

# Проверка наличия файлов
if not Path('data.csv').exists():
    print("Файл data.csv не найден. Сначала запустите: python generate_data.py")
    exit(1)
if not Path('data.parquet').exists():
    print("Файл data.parquet не найден. Сначала запустите: python generate_data.py")
    exit(1)
if not partitioned_path.exists():
    print("Директория data_partitioned не найдена. Сначала запустите: python generate_data.py")
    exit(1)

print("=" * 70)
print("Бенчмарк: производительность чтения и фильтрации")
print("=" * 70)
print()

# Чтение одной колонки
start = time.time()
pd.read_csv('data.csv', usecols=['age'])
csv_read_col = time.time() - start

start = time.time()
pd.read_parquet('data.parquet', columns=['age'])
parquet_read_col = time.time() - start

print(f"Чтение колонки 'age':")
print(f"  CSV:     {csv_read_col:.3f} с")
print(f"  Parquet: {parquet_read_col:.3f} с в {csv_read_col/parquet_read_col:.1f}x быстрее")

# Фильтрация + агрегация
start = time.time()
pd.read_csv('data.csv').query("city == 'Москва'").groupby('category')['amount'].sum()
csv_filter = time.time() - start

start = time.time()
pd.read_parquet('data.parquet').query("city == 'Москва'").groupby('category')['amount'].sum()
parquet_filter = time.time() - start

print(f"\nФильтр + агрегация (city == 'Москва'):")
print(f"  CSV:     {csv_filter:.3f} с")
print(f"  Parquet: {parquet_filter:.3f} с  в {csv_filter/parquet_filter:.1f}x быстрее")

# 7. Замеры чтения: фильтрация по году/месяцу (партиционирование)
print("\n" + "-" * 70)
print("Фильтрация по году и месяцу (год == '2024' & месяц == '01')")
print("-" * 70)

# Без партиционирования, читаем ВЕСЬ файл и фильтруем в памяти
start = time.time()
df_full = pd.read_parquet('data.parquet')
result1 = df_full.query("год == '2024' and месяц == '01'").groupby('category')['amount'].sum()
non_partitioned_filter = time.time() - start

# С партиционированием, читаем ТОЛЬКО нужную директорию
partition_path = partitioned_path / "год=2024" / "месяц=01"
start = time.time()
result2 = pd.read_parquet(partition_path).groupby('category')['amount'].sum()
partitioned_filter = time.time() - start

# Проверка корректности
assert abs((result1 - result2).sum()) < 0.01, "Результаты не совпадают!"

print(f"  Без партиций:  {non_partitioned_filter:6.3f} с  (читаем весь файл)")
print(f"  С партициями:  {partitioned_filter:6.3f} с  (читаем только папку год=2024/месяц=01)")
print(f"  Ускорение:     в {non_partitioned_filter/partitioned_filter:5.1f}x быстрее")


print("\n" + "=" * 60)
print("Сравнение скорости работы CSV vs Parquet закончено")
print("=" * 60)