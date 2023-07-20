-- Создание схемы
CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE dds.shop (
    pos VARCHAR(50) PRIMARY KEY,
    pos_name VARCHAR(50)
);

CREATE TABLE dds.tran_shop (
    transaction_id VARCHAR(50) PRIMARY KEY,
    pos VARCHAR(50),
    FOREIGN KEY (pos) REFERENCES dds.shop (pos)
);

-- Создание таблицы "brand" с первичным ключом "brand_id"
CREATE TABLE dds.brand (
    brand_id VARCHAR(50) PRIMARY KEY,
    brand VARCHAR(255)
);

-- Создание таблицы "category" с первичным ключом "category_id"
CREATE TABLE dds.category (
    category_id VARCHAR(50) PRIMARY KEY,
    category_name VARCHAR(50)
);

-- Создание таблицы "product" с первичным ключом "product_id"
CREATE TABLE dds.product (
    product_id VARCHAR(50) PRIMARY KEY,
    name_short VARCHAR(255),
    category_id VARCHAR(50),
    pricing_line_id VARCHAR(50),
    brand_id VARCHAR(50),   
    FOREIGN KEY (category_id) REFERENCES dds.category (category_id),
    FOREIGN KEY (brand_id) REFERENCES dds.brand (brand_id)
);

-- Создание таблицы "stock"
CREATE TABLE dds.stock (
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
CREATE TABLE dds.transaction (
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