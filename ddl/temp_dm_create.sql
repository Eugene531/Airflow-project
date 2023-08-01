-- Создание схемы "temp_dm" для хранения данных Data Mart

-- Создание таблицы "transactions_group_ymd" для хранения агрегированных данных по транзакциям, сгруппированным по дате
CREATE TABLE IF NOT EXISTS temp_dm.transactions_group_ymd (
    date TEXT,                      -- Дата
    day INTEGER,                    -- День недели (числовое значение)
    pos VARCHAR(50),                -- Уникальный ID магазина (POS)
    category_name VARCHAR(50),      -- Название категории продукта
    name_short VARCHAR(255),        -- Название продукта
    sum_price BIGINT,               -- Цена за день
    sum_quantity BIGINT,            -- Общее количество проданных продуктов в конкретный день недели
    UPDATE_DATE DATE                -- Дата обновления записи
);

-- Создание таблицы "average_check" для хранения среднего чека по магазинам и датам
CREATE TABLE IF NOT EXISTS temp_dm.average_check (
    date VARCHAR(10),               -- Дата
    pos VARCHAR(50),                -- Уникальный ID магазина (POS)
    avg NUMERIC(10,2),              -- Средний чек
    UPDATE_DATE DATE                -- Дата обновления записи
);

-- Создание таблицы "purchases" для хранения информации о закупках продуктов
CREATE TABLE IF NOT EXISTS temp_dm.purchases (
    product_id VARCHAR(50),         -- Уникальный ID продукта
    update_date_in_stock DATE,      -- Дата последнего обновления запасов продукта
    stock_total NUMERIC(10,2),      -- Общее количество продукта в запасах
    cost_per_item NUMERIC(10,2),    -- Цена продукта за единицу
    name_short VARCHAR(255),        -- Название продукта
    category VARCHAR(50),           -- Категория продукта
    sold_total NUMERIC(10,2),       -- Общее количество проданного продукта
    UPDATE_DATE DATE                -- Дата обновления записи
);

-- Создание таблицы "mean_monthly_product_stats" для хранения средних месячных статистических данных по продуктам
CREATE TABLE IF NOT EXISTS temp_dm.mean_monthly_product_stats (
    product_id VARCHAR(50),         -- Уникальный ID продукта
    date VARCHAR(10),               -- Дата
    quantity NUMERIC(10,2),         -- Среднее количество продуктов, проданных в месяц
    avg_price NUMERIC(10,2),        -- Средняя цена продукта в месяц
    UPDATE_DATE DATE                -- Дата обновления записи
);

-- Создание таблицы "total_stats" для хранения общей статистики по продажам
CREATE TABLE IF NOT EXISTS temp_dm.total_stats (
    date DATE,                      -- Дата
    total_gross NUMERIC(10,2),      -- Общая выручка за месяц
    total_volume NUMERIC(10,2),     -- Общий объем продаж за месяц
    avg_check NUMERIC(10,2),        -- Средний чек за месяц
    UPDATE_DATE DATE                -- Дата обновления записи
);
