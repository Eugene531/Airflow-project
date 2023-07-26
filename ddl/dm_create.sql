-- Создание схемы
CREATE SCHEMA IF NOT EXISTS dm;

-- Создание таблицы "transactions_group_ymd"
CREATE TABLE IF NOT EXISTS dm.transactions_group_ymd (
    date TEXT,
    day INTEGER,
    pos VARCHAR(50),
    category_name VARCHAR(50),
    name_short VARCHAR(255),
    sum_price BIGINT,
    sum_quantity BIGINT,
    UPDATE_DATE DATE
);

-- Создание таблицы "average_check"
CREATE TABLE IF NOT EXISTS dm.average_check (
    date VARCHAR(10),
    pos VARCHAR(50),
    avg NUMERIC(10,2),
    UPDATE_DATE DATE
);

-- Создание таблицы "purchases"
CREATE TABLE IF NOT EXISTS dm.purchases (
    product_id VARCHAR(50),
    update_date_in_stock DATE,
    stock_total NUMERIC(10,2),
    cost_per_item NUMERIC(10,2),
    name_short VARCHAR(255),
    category VARCHAR(50),
    sold_total NUMERIC(10,2),
    UPDATE_DATE DATE
);

-- Создание таблицы "mean_monthly_product_stats"
CREATE TABLE IF NOT EXISTS dm.mean_monthly_product_stats (
    product_id VARCHAR(50),
    date VARCHAR(10),
    quantity NUMERIC(10,2),
    avg_price NUMERIC(10,2),
    UPDATE_DATE DATE
);

-- Создание таблицы "total_stats"
CREATE TABLE IF NOT EXISTS dm.total_stats (
    date VARCHAR(10),
    total_gross NUMERIC(10,2),
    total_volume NUMERIC(10,2),
    avg_check NUMERIC(10,2),
    UPDATE_DATE DATE
);
