-- Создание схемы "dds"
CREATE SCHEMA IF NOT EXISTS dds;

-- Создание таблицы "shop" для хранения информации о магазинах
CREATE TABLE IF NOT EXISTS dds.shop (
   pos VARCHAR(50) PRIMARY KEY,                -- ID магазина
   pos_name VARCHAR(50) NOT NULL               -- Название магазина (не может быть пустым)
);

-- Создание таблицы "tran_shop" для хранения информации о транзакциях и связанных с ними магазинах
CREATE TABLE IF NOT EXISTS dds.tran_shop (
   transaction_id VARCHAR(50) PRIMARY KEY,     -- ID транзакции
   pos VARCHAR(50) NOT NULL,                   -- ID магазина, связанный с транзакцией (не может быть пустым)
   FOREIGN KEY (pos) REFERENCES dds.shop (pos)
);

-- Создание таблицы "brand" для хранения информации о брендах продуктов
CREATE TABLE IF NOT EXISTS dds.brand (
   brand_id VARCHAR(50) PRIMARY KEY,           -- ID бренда
   brand VARCHAR(255) NOT NULL                 -- Название бренда (не может быть пустым)
);

-- Создание таблицы "category" для хранения информации о категориях продуктов
CREATE TABLE IF NOT EXISTS dds.category (
   category_id VARCHAR(50) PRIMARY KEY,        -- ID категории
   category_name VARCHAR(50) NOT NULL          -- Название категории (не может быть пустым)
);

-- Создание таблицы "product" для хранения информации о продуктах
CREATE TABLE IF NOT EXISTS dds.product (
   product_id VARCHAR(50) PRIMARY KEY,         -- ID продукта
   name_short VARCHAR(255) NOT NULL,           -- Название продукта (не может быть пустым)
   category_id VARCHAR(50),                    -- ID категории продукта
   pricing_line_id VARCHAR(50),                -- ID линии ценообразования продукта
   brand_id VARCHAR(50),                       -- ID бренда продукта
   FOREIGN KEY (category_id) REFERENCES dds.category (category_id),
   FOREIGN KEY (brand_id) REFERENCES dds.brand (brand_id)
);

-- Создание таблицы "stock" для хранения информации о запасах продуктов
CREATE TABLE IF NOT EXISTS dds.stock (
   available_on DATE,                          -- Дата доступности продукта
   product_id VARCHAR(50),                     -- ID продукта
   pos VARCHAR(50) NOT NULL,                   -- ID магазина (не может быть пустым)
   available_quantity NUMERIC (10,3) NOT NULL, -- Доступное количество продукта (не может быть пустым)
   cost_per_item NUMERIC (10,3) NOT NULL,      -- Цена продукта за единицу (не может быть пустой)
   PRIMARY KEY (available_on, product_id, pos),
   FOREIGN KEY (pos) REFERENCES dds.shop (pos),
   FOREIGN KEY (product_id) REFERENCES dds.product (product_id)
);

-- Создание таблицы "transaction" для хранения информации о транзакциях продажи продуктов
CREATE TABLE IF NOT EXISTS dds.transaction (
   transaction_id VARCHAR(50),                 -- ID транзакции
   product_id VARCHAR(50),                     -- ID продукта
   recorded_on TIMESTAMP NOT NULL,             -- Время записи транзакции (не может быть пустым)
   quantity INTEGER NOT NULL,                  -- Количество продуктов в транзакции (не может быть пустым)
   price INTEGER NOT NULL,                     -- Цена продукта за единицу в транзакции (не может быть пустой)
   price_full INTEGER NOT NULL,                -- Полная цена продукта в транзакции (не может быть пустой)
   order_type_id VARCHAR(50) NOT NULL,         -- Тип транзакции (не может быть пустым)
   PRIMARY KEY (transaction_id, product_id),
   FOREIGN KEY (product_id) REFERENCES dds.product (product_id),
   FOREIGN KEY (transaction_id) REFERENCES dds.tran_shop (transaction_id)
);

-- Создание таблицы "product_errors" для хранения ошибок при обработке данных о продуктах
CREATE TABLE IF NOT EXISTS dds.product_errors (
   product_id VARCHAR(50) NOT NULL,            -- ID продукта, в котором возникла ошибка (не может быть пустым)
   name_short VARCHAR(50),                     -- Название продукта
   category_id VARCHAR(50),                    -- ID категории продукта
   pricing_line_id VARCHAR(50),                -- ID линии ценообразования продукта
   brand_id VARCHAR(50),                       -- ID бренда продукта
   name_short_error VARCHAR(50) NOT NULL       -- Ошибка в названии продукта (не может быть пустой)
);

-- Создание таблицы "stock_errors" для хранения ошибок при обработке данных о запасах продуктов
CREATE TABLE IF NOT EXISTS dds.stock_errors (
   available_on DATE NOT NULL,                 -- Дата доступности продукта (не может быть пустой)
   product_id VARCHAR(50) NOT NULL,            -- ID продукта (не может быть пустым)
   pos VARCHAR(50) NOT NULL,                   -- ID магазина (не может быть пустым)
   available_quantity VARCHAR(50),             -- Доступное количество продукта (строковый тип данных)
   cost_per_item VARCHAR(50),                  -- Цена продукта за единицу (строковый тип данных)
   product_id_error VARCHAR(50) NOT NULL,      -- Ошибка в id продукта (не может быть пустой)
   available_quantity_error VARCHAR(50),       -- Ошибка в доступном количестве продукта (строковый тип данных)
   cost_per_item_error VARCHAR(50),            -- Ошибка в цене продукта за единицу (строковый тип данных)
   pos_error VARCHAR(50)                       -- Ошибка в id магазина (строковый тип данных)
);

-- Создание таблицы "transaction_errors" для хранения ошибок при обработке данных о транзакциях продажи продуктов
CREATE TABLE IF NOT EXISTS dds.transaction_errors (
   transaction_id VARCHAR(50) NOT NULL,        -- ID транзакции (не может быть пустым)
   product_id VARCHAR(50) NOT NULL,            -- ID продукта (не может быть пустым)
   recorded_on VARCHAR(50) NOT NULL,           -- Время записи транзакции (не может быть пустым)
   quantity VARCHAR(50),                       -- Количество продуктов в транзакции (строковый тип данных)
   price VARCHAR(50),                          -- Цена продукта за единицу в транзакции (строковый тип данных)
   price_full VARCHAR(50),                     -- Полная цена продукта в транзакции (строковый тип данных)
   order_type_id VARCHAR(50),                  -- Тип транзакции (строковый тип данных)
   quantity_error VARCHAR(50),                 -- Ошибка в количестве продуктов (строковый тип данных)
   price_error VARCHAR(50),                    -- Ошибка в цене продукта за единицу (строковый тип данных)
   product_id_error VARCHAR(50),               -- Ошибка в id продукта (строковый тип данных)
   transaction_id_error VARCHAR(50)            -- Ошибка в id транзакции (строковый тип данных)
); 