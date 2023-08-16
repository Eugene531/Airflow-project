import enum


class ProcessParams(enum.Enum):
    read_schema = 'dds'
    write_schema = 'dm'
    module = 'dds_to_dm'

    # Список таблиц для чтения из исходной БД
    read_tables = [
        'brand',               # Таблица с данными о брендах
        'category',            # Таблица с данными о категориях
        'product',             # Таблица с данными о продуктах
        'stock',               # Таблица с данными о складах
        'transaction',         # Таблица с данными о транзакциях
    ]

    # Список таблиц для записи в целевую БД
    write_tables = [
        'transactions_group_ymd',
        'average_check',
        'purchases',
        'mean_monthly_product_stats',
        'total_stats',
    ]

    # Параметры для чтения данных из исходной БД
    read_db_data = {
        'config': '',
        'tables': read_tables,
        'schema': read_schema,
    }

    # Параметры для записи данных в целевую БД
    write_db_data = {
        'config': '',
        'tables': write_tables,
        'schema': write_schema,
    }
