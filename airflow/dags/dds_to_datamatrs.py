import sys

from airflowignore_files.etl_data_from_dds_to_dm import EtlProcessorDdsToDm


def run_etl_process(sources_conn_params: str, writer_conn_params: str) -> None:
    """
    Запускает процесс ETL (extract, transform, load) между двумя базами данных.

    Входные параметры:
        sources_conn_params: str
            Параметры подключения к исходной БД для чтения информации.
        writer_conn_params: str
            Параметры подключения к целевой БД для записи.

    Примечание:
    Входные данные для подключения должны быть представлены в формате URI:
        'postgresql://<username>:<password>@<host>:<port>/<database>'
    """

    # Создаем экземпляр класса EtlProcessorSourcesToDds для обработки ETL-процесса.
    etl = EtlProcessorDdsToDm(sources_conn_params, writer_conn_params)
    
    # Извлекаем данные из исходной БД в схеме 'sources'.
    data = etl.extract_data_from_dds()
    data = etl.transform_data_from_sources_db(data)
    print(data)

    # # Преобразуем данные из исходной БД в схеме 'sources'.
    # transformed_data = etl.transform_data_from_dds_db(data)

    # # Загружаем преобразованные данные в целевую БД схемы 'dds'.
    # etl.load_data_to_dm(transformed_data)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Нужно передать данные подключения к двум БД (1 - read, 2 - write)")
        sys.exit(1)

    sources_db_conn_params = 'postgresql://interns_5:0XcptM@10.1.108.29:5432/internship_sources'#sys.argv[1]
    writer_db_conn_params = 'postgresql://interns_5:0XcptM@10.1.108.29:5432/internship_5_db'#sys.argv[2]

    run_etl_process(writer_db_conn_params, writer_db_conn_params)

#'postgresql://interns_5:0XcptM@10.1.108.29:5432/internship_5_db'
#'postgresql://interns_5:0XcptM@10.1.108.29:5432/internship_sources'
