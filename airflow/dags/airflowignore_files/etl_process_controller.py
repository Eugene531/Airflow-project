from typing import Union

from .connectors_to_schem.conn_to_dds import ConnectorToDds
from .connectors_to_schem.conn_to_sources import ConnectorToSources
from .connectors_to_schem.conn_to_dm import ConnectorToDm
from .transform_rules.transform_sources_to_dds import TransformRulesSourcesToDds
from .transform_rules.transform_dds_to_dm import TransformRulesDdsToDm


class EtlProcessorController:
    """
    """

    def __init__(
            self,
            sources_conn_params: str,
            writer_conn_params: str,
            conn_to_sources: Union[ConnectorToDds, ConnectorToSources, ConnectorToDm],
            conn_to_writer: Union[ConnectorToDds, ConnectorToSources, ConnectorToDm],
            transform_rules: Union[TransformRulesSourcesToDds, TransformRulesDdsToDm]
        ) -> None:
        
        self.__sources_conn_params = sources_conn_params
        self.__writer_conn_params = writer_conn_params
        self.__conn_to_sources = conn_to_sources
        self.__conn_to_writer = conn_to_writer
        self.__transform_rules = transform_rules


    def extract_data_from_sources_db(self) -> dict:
        """
        Метод для извлечения (Extract) данных из исходной БД в схеме 'sources'.

        Returns:
        Словарь с данными таблиц исходной БД.
        """
        
        # Инициализируем переменную sources_data, чтобы хранить данные исходной БД.
        extract_data = None

        # Создаем объект ConnectorToSourcesSchema и используем менеджер контекста with
        # для обеспечения автоматического закрытия соединения после использования.
        with self.__conn_to_sources(self.__sources_conn_params) as sources:

            # Используем метод get_tables_data(), чтобы получить данные таблиц исходной БД.
            dds_data = sources.get_tables_data()

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
    

    def load_data_to_writer_db(self, transformed_data: dict) -> None:
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

