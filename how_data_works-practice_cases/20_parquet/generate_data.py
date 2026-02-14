import pandas as pd
import numpy as np
import time
from pathlib import Path
import shutil

# 1. Генерация данных
np.random.seed(42)
n = 10_000_000

cities = ['Москва', 'СПб', 'Новосибирск', 'Екатеринбург', 'Казань', 'Краснодар']
df = pd.DataFrame({
    'user_id': np.arange(n),
    'name': np.random.choice(['Алиса', 'Марат', 'Артур', 'Даша', 'Егор'], n),
    'age': np.random.randint(18, 70, n),
    'city': np.random.choice(cities, n),
    'amount': np.round(np.random.exponential(500, n), 2),
    'category': np.random.choice(['еда', 'транспорт', 'развлечения', 'быт'], n),
    'timestamp': pd.date_range('2024-01-01', periods=n, freq='min')
})

# Извлекаем год и месяц для партиционирования
df['год'] = df['timestamp'].dt.year
df['месяц'] = df['timestamp'].dt.month.astype(str).str.zfill(2)  # 01, 02, ...

print("=" * 60)
print("Генерация данных: CSV и Parquet")
print("=" * 60)

# 2. Запись: CSV
start = time.time()
df.to_csv('data.csv', index=False)
csv_write_time = time.time() - start
csv_size = Path('data.csv').stat().st_size / 1024**2

# 3. Запись: Parquet (без партиционирования)
start = time.time()
df.to_parquet('data.parquet', engine='pyarrow', compression='snappy')
parquet_write_time = time.time() - start
parquet_size = Path('data.parquet').stat().st_size / 1024**2

# 4. Запись: Parquet с партиционированием
partitioned_path = Path('data_partitioned')
if partitioned_path.exists():
    shutil.rmtree(partitioned_path)

start = time.time()
df.to_parquet(
    partitioned_path,
    partition_cols=['год', 'месяц'],
    engine='pyarrow',
    compression='snappy'
)
partitioned_write_time = time.time() - start

# Подсчёт размера партиционированных файлов
partitioned_size = sum(f.stat().st_size for f in partitioned_path.rglob('*.parquet')) / 1024**2

print("=" * 60)
print("Генерация завершена!")
print("=" * 60)

print("\nРазмер файлов и время записи:")
print(f"  CSV:                {csv_size:6.1f} МБ  | запись: {csv_write_time:.3f} с")
print(f"  Parquet:            {parquet_size:6.1f} МБ  | запись: {parquet_write_time:.3f} с")
print(f"  Parquet (партиции): {partitioned_size:6.1f} МБ  | запись: {partitioned_write_time:.3f} с")

# 5. Структура партиционированных файлов
SHOW_TREE = False
if SHOW_TREE:
    print("\nСтруктура партиционированных данных:")
    for year_dir in sorted(partitioned_path.glob('год=*')):
        print(f"  {year_dir.name}/")
        for month_dir in sorted(year_dir.glob('месяц=*')):
            files = list(month_dir.glob('*.parquet'))
            print(f"    └── {month_dir.name}/ ({len(files)} файлов, {sum(f.stat().st_size for f in files) / 1024**2:.1f} МБ)")

