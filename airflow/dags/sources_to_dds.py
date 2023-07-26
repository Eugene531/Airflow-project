import sys

from airflowignore_files.etl_process_controller import EtlProcessorController
from airflowignore_files.conn_to_schem.to_sources import ConnectorToSources
from airflowignore_files.conn_to_schem.to_dds import ConnectorToDds
from airflowignore_files.transform_rules.sources_to_dds \
    import TransformSourcesToDds


def run_etl_process(sources_conn_params: str, writer_conn_params: str) -> None:
    """
    Запускает процессы Extract, Transform, Load между схемами: 'sources' и 'dds'.

    Входные параметры:
        sources_conn_params: str
            Параметры подключения к исходной БД для чтения информации.
        writer_conn_params: str
            Параметры подключения к целевой БД для записи.

    Примечание:
    Входные данные для подключения должны быть представлены в формате URI:
        'postgresql://<username>:<password>@<host>:<port>/<database>'
    """

    # Создаем экземпляр класса EtlProcessorController для обработки ETL-процесса.
    etl = EtlProcessorController(
        sources_conn_params, 
        writer_conn_params,
        ConnectorToSources,
        ConnectorToDds,
        TransformSourcesToDds
        )
    
    # Извлекаем данные из исходной БД.
    data = etl.extract_data_from_sources_db()

    # Преобразуем данные из исходной БД по правилам из 'TransformSourcesToDds'.
    transformed_data = etl.transform_data_from_sources_db(data)

    # Загружаем преобразованные данные в целевую БД.
    etl.load_data_to_writer_db(transformed_data)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Нужно передать данные подключения к двум БД (1 - read, 2 - write)")
        sys.exit(1)

    sources_db_conn_params = sys.argv[1]
    writer_db_conn_params = sys.argv[2]

    run_etl_process(sources_db_conn_params, writer_db_conn_params)
