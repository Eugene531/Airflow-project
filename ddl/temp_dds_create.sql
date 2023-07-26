-- Создание схемы
CREATE SCHEMA IF NOT EXISTS temp_dds;

CREATE TABLE IF NOT EXISTS temp_dds.shop (
    pos VARCHAR(50) PRIMARY KEY,
    pos_name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS temp_dds.tran_shop (
    transaction_id VARCHAR(50) PRIMARY KEY,
    pos VARCHAR(50),
    FOREIGN KEY (pos) REFERENCES dds.shop (pos)
);

-- Создание таблицы "brand" с первичным ключом "brand_id"
CREATE TABLE IF NOT EXISTS temp_dds.brand (
    brand_id VARCHAR(50) PRIMARY KEY,
    brand VARCHAR(255)
);

-- Создание таблицы "category" с первичным ключом "category_id"
CREATE TABLE IF NOT EXISTS temp_dds.category (
    category_id VARCHAR(50) PRIMARY KEY,
    category_name VARCHAR(50)
);

-- Создание таблицы "product" с первичным ключом "product_id"
CREATE TABLE IF NOT EXISTS temp_dds.product (
    product_id VARCHAR(50) PRIMARY KEY,
    name_short VARCHAR(255),
    category_id VARCHAR(50),
    pricing_line_id VARCHAR(50),
    brand_id VARCHAR(50),   
    FOREIGN KEY (category_id) REFERENCES dds.category (category_id),
    FOREIGN KEY (brand_id) REFERENCES dds.brand (brand_id)
);

-- Создание таблицы "stock"
CREATE TABLE IF NOT EXISTS temp_dds.stock (
    available_on DATE,
    product_id VARCHAR(50),
    pos VARCHAR(50),
    available_quantity NUMERIC (10,3),
    cost_per_item NUMERIC (10,3),
    PRIMARY KEY (available_on, product_id, pos),
    FOREIGN KEY (pos) REFERENCES dds.shop (pos),
    FOREIGN KEY (product_id) REFERENCES dds.product (product_id)
);

-- Создание таблицы "transaction"
CREATE TABLE IF NOT EXISTS temp_dds.transaction (
    transaction_id VARCHAR(50),
    product_id VARCHAR(50),
    recorded_on TIMESTAMP,
    quantity INTEGER,
    price INTEGER,
    price_full INTEGER,
    order_type_id VARCHAR(50),
    PRIMARY KEY (transaction_id, product_id),
    FOREIGN KEY (product_id) REFERENCES dds.product (product_id),
    FOREIGN KEY (transaction_id) REFERENCES dds.tran_shop (transaction_id)
);


-- Создание таблицы "product_errors"
CREATE TABLE IF NOT EXISTS temp_dds.product_errors (
    product_id VARCHAR(50),
    name_short VARCHAR(50),
    category_id VARCHAR(50),
    pricing_line_id VARCHAR(50),
    brand_id VARCHAR(50),
    name_short_error VARCHAR(50)
);

-- Создание таблицы "stock_errors"
CREATE TABLE IF NOT EXISTS temp_dds.stock_errors (
    available_on VARCHAR(50),
    product_id VARCHAR(50),
    pos VARCHAR(50),
    available_quantity VARCHAR(50),
    cost_per_item VARCHAR(50),
    product_id_error VARCHAR(50),
    available_quantity_error VARCHAR(50),
    cost_per_item_error VARCHAR(50),
    pos_error VARCHAR(50)
);

-- Создание таблицы "transaction_errors"
CREATE TABLE IF NOT EXISTS temp_dds.transaction_errors (
    transaction_id VARCHAR(50),
    product_id VARCHAR(50),
    recorded_on VARCHAR(50),
    quantity VARCHAR(50),
    price VARCHAR(50),
    price_full VARCHAR(50),
    order_type_id VARCHAR(50),
    quantity_error VARCHAR(50),
    price_error VARCHAR(50),
    product_id_error VARCHAR(50),
    transaction_id_error VARCHAR(50)
);