# generate_clients.py
import random
from datetime import datetime, timedelta

# Данные для генерации
surnames = ["Иванов", "Петров", "Сидоров", "Смирнов", "Кузнецов", "Попов", "Лебедев", "Волков", "Козлов", "Новиков"]
names = ["Александр", "Дмитрий", "Михаил", "Сергей", "Андрей", "Алексей", "Евгений", "Денис", "Илья", "Владимир"]
patronomics = ["Александрович", "Дмитриевич", "Михайлович", "Сергеевич", "Андреевич", "Алексеевич", "Евгеньевич", "Денисович", "Ильич", "Владимирович"]
cities = ["Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург", "Казань", "Нижний Новгород", "Челябинск", "Самара", "Омск", "Ростов-на-Дону"]

# Генерация дат: от сегодня - 5 до сегодня - 1 (включительно)
today = datetime.now().date()
start_date = today - timedelta(days=5)
end_date = today - timedelta(days=1)

current_date = start_date
dates = []

while current_date <= end_date:
    dates.append(current_date)
    current_date += timedelta(days=1)

# Проверка: должно быть 5 дней
print(f"Генерация данных за период: {start_date} — {end_date}")
print(f"Количество дней: {len(dates)}")

# Генерация 50 записей (по 10 на день)
records = []
for date in dates:
    for _ in range(10):
        record = {
            "ID": len(records) + 1,
            "Фамилия": random.choice(surnames),
            "Имя": random.choice(names),
            "Отчество": random.choice(patronomics),
            "Город": random.choice(cities),
            "Дата внесения": date.strftime("%Y-%m-%d")
        }
        records.append(record)

# Сохранение в файл
filename = "clients_data.csv"
with open(filename, "w", encoding="utf-8") as f:
    f.write("ID,surname,name,patronymic,city,entry_date\n")
    for r in records:
        line = f"{r['ID']},{r['Фамилия']},{r['Имя']},{r['Отчество']},{r['Город']},{r['Дата внесения']}"
        f.write(line + "\n")

print(f"Файл {filename} с {len(records)} записями успешно создан.")