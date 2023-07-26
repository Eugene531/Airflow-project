from .conn_to_datamarts import ConnectorToDatamartsSchema


class EtlProcessorDdsToDm:
    """
    Используется для реализации процесса ETL (extract, transform, load) между 2 БД.
    
    Для этого проводятся процессы выгрузки и обработки данных из схемы 'dds'
    исходной БД и загрузки их в схему 'dm' БД для записи.

    Входные параметры:
        sources_conn_params: str
            Параметры подключения к схеме 'dds' исходной БД для чтения информации.
        writer_conn_params: str
            Параметры подключения к схеме 'dm' БД для записи.

    Методы:
        extract_data_from_sources_db() -> dict:
            Метод для извлечения (Extract) данных из исходной БД в схеме 'dds'.
        transform_data_from_sources_db(sources_data: dict) -> dict
            Метод для преобразования (Transform) данных из исходной БД в схеме 'dds'.
        load_data_to_writer_db(transformed_data: dict) -> None
            Метод для загрузки (Load) преобразованных данных в схему 'dm'.
    """

    def __init__(self, sources_conn_params: str, writer_conn_params: str) -> None:
        self.__sources_conn_params: str = sources_conn_params
        self.__writer_conn_params: str = writer_conn_params


    def extract_data_from_dds(self) -> dict:
        """
        Метод для извлечения (Extract) данных из исходной БД в схеме 'sources'.

        Returns:
        Словарь с данными таблиц исходной БД.
        """
        
        # Инициализируем переменную sources_data, чтобы хранить данные исходной БД.
        dds_data = None

        # Создаем объект ConnectorToSourcesSchema и используем менеджер контекста with
        # для обеспечения автоматического закрытия соединения после использования.
        with ConnectorToDatamartsSchema(self.__sources_conn_params) as sources:

            # Используем метод get_tables_data(), чтобы получить данные таблиц исходной БД.
            dds_data = sources.get_data_from_dds()

        # Возвращаем полученные данные таблиц исходной БД.
        return dds_data


    def transform_data_from_sources_db(self, sources_data: dict) -> dict:
        """
        Метод для преобразования (Transform) данных из исходной БД в схеме 'sources'.

        Входные параметры:
        sources_data: dict
            Словарь с данными таблиц исходной БД.

        Returns:
        Словарь с преобразованными данными для дальнейшей записи в схему 'dds'.
        """
        
        # Инициализируем переменную, чтобы хранить преобразованные данные.
        transformed_data = None
        
        # Создаем объект ConnectorToDdsSchema и используем менеджер контекста with
        # для обеспечения автоматического закрытия соединения после использования.
        with ConnectorToDatamartsSchema(self.__writer_conn_params) as dds:

            # Используем метод get_transformed_data() объекта dds, чтобы получить
            # преобразованные данные, которые будут загружены в схему 'dds'.
            transformed_data = dds.get_transformed_data(sources_data)
        
        # Возвращаем преобразованные данные
        return transformed_data
    

    # def load_data_to_writer_db(self, transformed_data: dict) -> None:
    #     """
    #     Метод для загрузки (Load) преобразованных данных в схему 'dds'.

    #     Входные параметры:
    #     transformed_data: dict
    #         Словарь с преобразованными данными для таблиц в схеме 'dds'.
    #     """

    #     # Создаем объект ConnectorToDdsSchema и используем менеджер контекста with
    #     # для обеспечения автоматического закрытия соединения после использования.
    #     with ConnectorToDdsSchema(self.__writer_conn_params) as dds:

    #         # Используем метод load_transformed_data() объекта dds для загрузки
    #         # преобразованных данных (transformed_data) в схему 'dds'.
    #         dds.load_transformed_data(transformed_data)

