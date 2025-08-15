-- Создание таблицы клиентов
CREATE TABLE IF NOT EXISTS clients (
    client_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    address TEXT,
    registration_date DATE NOT NULL DEFAULT CURRENT_DATE
);