import sys

from airflowignore_files.etl_process_controller import EtlProcessorController


def run_etl(read_db_data: str, write_db_data: str, modul: str) -> None:
    """
    Запускает процессы ETL (Extract, Transform, Load) между 'sources' и 'dds'.

    Входные параметры:
        read_db_data: dict
            Параметры для взаимодействия с БД для чтения в формате: {
                'config': данные для подключения к БД в форме URI, 
                'tables': таблицы для чтения данных,
                'schema': схема БД (необязательный параметр, поумолчанию public)
                }.
        write_db_data: dict
            Параметры для взаимодействия с БД для записи в формате: {
                'config': данные для подключения к БД в форме URI, 
                'tables': таблицы для записи данных,
                'schema': схема БД (необязательный параметр, поумолчанию public)
                }.
        modul: str
            Имя модуля с правилами трансформации.

    Примечание:
        Входные данные для подключения должны быть представлены в формате URI:
            'postgresql://<username>:<password>@<host>:<port>/<database>'
    """

    # Создаем экземпляр класса EtlProcessorController для обработки ETL-процесса.
    etl = EtlProcessorController(read_db_data, write_db_data, modul)
    
    # Извлекаем сырые данные
    data = etl.extract_data()

    # Преобразуем сырые данные по правилам из модуля 'modul'
    transformed_data = etl.transform_data(data)

    # Загружаем преобразованные данные
    etl.load_data(transformed_data)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Нужно передать данные подключения к двум БД (1 - read, 2 - write)")
        sys.exit(1)

    read_db_conn_params = sys.argv[1]
    write_db_conn_params = sys.argv[2]

    schema_read = 'sources'
    schema_write = 'dds'
    modul = 'sources_to_dds'
    
    # Список таблиц для чтения/записи данных
    tables = [
        'brand',               # Таблица с данными о брендах
        'category',            # Таблица с данными о категориях
        'product',             # Таблица с данными о продуктах
        'stock',               # Таблица с данными о складах
        'transaction',         # Таблица с данными о транзакциях
    ]

    # Параметры для чтения данных из исходной БД
    read_db_data = {
        'config': read_db_conn_params,
        'tables': tables,
        'schema': schema_read,
    }

    # Параметры для записи данных в целевую БД
    write_db_data = {
        'config': write_db_conn_params,
        'tables': tables,
        'schema': schema_write,
    }

    # Запускаем ETL-процесс
    run_etl(read_db_data, write_db_data, modul)
