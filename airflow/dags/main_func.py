import sys
from importlib import import_module

from airflowignore_files.etl_controller.etl_process_controller import EtlProcessorController


def run_etl(read_db_data: dict, write_db_data: dict, module: str) -> None:
    """
    Запускает процессы ETL (Extract, Transform, Load) между двумя БД.

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
        module: str
            Имя модуля с правилами трансформации.

    Примечание:
        Входные данные для подключения должны быть представлены в формате URI:
            'postgresql://<username>:<password>@<host>:<port>/<database>'
    """

    # Создаем экземпляр класса EtlProcessorController для обработки ETL-процесса.
    etl = EtlProcessorController(read_db_data, write_db_data, module)
    
    # Извлекаем сырые данные
    data = etl.extract_data()

    # Преобразуем сырые данные по правилам из 'module'
    transformed_data = etl.transform_data(data)

    # Загружаем преобразованные данные
    etl.load_data(transformed_data)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Нужно передать следующие данные:\n"
              "1 - данные для подключения к БД для чтения;\n"
              "2 - данные для подключения к БД для записи;\n"
              "3 - имя файла, в котором определен класс ProcessParams (enum).")
        sys.exit(1)

    read_db_conn_params = sys.argv[1]
    write_db_conn_params = sys.argv[2]

    process_params_path = f'airflowignore_files.enums.{sys.argv[3]}'
    ProcessParams = import_module(process_params_path).ProcessParams
    
    ProcessParams.read_db_data.value['config'] = read_db_conn_params
    ProcessParams.write_db_data.value['config'] = write_db_conn_params

    # Запускаем ETL-процесс
    run_etl(
        ProcessParams.read_db_data.value, 
        ProcessParams.write_db_data.value, 
        ProcessParams.module.value
        )
