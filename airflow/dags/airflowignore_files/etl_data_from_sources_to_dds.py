from .conn_to_sources import ConnectorToSourcesSchema
from .conn_to_dds import ConnectorToDdsSchema


class EtlProcessorSourcesToDds:
    """
    Используется для реализации процесса ETL (extract, transform, load) между 2 БД.
    
    Для этого проводятся процессы выгрузки и обработки данных из схемы 'sources'
    исходной БД и загрузки их в схему 'dds' БД для записи.

    Входные параметры:
        sources_conn_params: str
            Параметры подключения к схеме 'sources' исходной БД для чтения информации.
        writer_conn_params: str
            Параметры подключения к схеме 'dds' БД для записи.

    Методы:
        extract_data_from_sources_db() -> dict:
            Метод для извлечения (Extract) данных из исходной БД в схеме 'sources'.
        transform_data_from_sources_db(sources_data: dict) -> dict
            Метод для преобразования (Transform) данных из исходной БД в схеме 'sources'.
        load_data_to_sources(transformed_data: dict) -> None
            Метод для загрузки (Load) преобразованных данных в схему 'dds'.
    """

    def __init__(self, sources_conn_params: str, writer_conn_params: str) -> None:
        self.__sources_conn_params: str = sources_conn_params
        self.__writer_conn_params: str = writer_conn_params


    def extract_data_from_sources_db(self) -> dict:
        """
        Метод для извлечения (Extract) данных из исходной БД в схеме 'sources'.

        Returns:
        Словарь с данными таблиц исходной БД.
        """
        
        # Инициализируем переменную sources_data, чтобы хранить данные исходной БД.
        sources_data = None

        # Создаем объект ConnectorToSourcesSchema и используем менеджер контекста with
        # для обеспечения автоматического закрытия соединения после использования.
        with ConnectorToSourcesSchema(self.__sources_conn_params) as sources:

            # Используем метод get_tables_data(), чтобы получить данные таблиц исходной БД.
            sources_data = sources.get_tables_data()

        # Возвращаем полученные данные таблиц исходной БД.
        return sources_data


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
        with ConnectorToDdsSchema(self.__writer_conn_params, sources_data) as dds:

            # Используем метод get_transformed_data() объекта dds, чтобы получить
            # преобразованные данные, которые будут загружены в схему 'dds'.
            transformed_data = dds.get_transformed_data()
        
        # Возвращаем преобразованные данные
        return transformed_data
    

    def load_data_to_sources(self, transformed_data: dict) -> None:
        """
        Метод для загрузки (Load) преобразованных данных в схему 'dds'.

        Входные параметры:
        transformed_data: dict
            Словарь с преобразованными данными для таблиц в схеме 'dds'.
        """

        # Создаем объект ConnectorToDdsSchema и используем менеджер контекста with
        # для обеспечения автоматического закрытия соединения после использования.
        with ConnectorToDdsSchema(self.__writer_conn_params) as dds:

            # Используем метод load_transformed_data() объекта dds для загрузки
            # преобразованных данных (transformed_data) в схему 'dds'.
            dds.load_transformed_data(transformed_data)
