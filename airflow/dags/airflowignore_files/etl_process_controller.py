from typing import Union

from .conn_to_schem.to_dds import ConnectorToDds
from .conn_to_schem.to_sources import ConnectorToSources
from .conn_to_schem.to_dm import ConnectorToDm
from .transform_rules.sources_to_dds import TransformSourcesToDds
from .transform_rules.dds_to_dm import TransformDdsToDm


class EtlProcessorController:
    """
    Используется для управления процессами ETL (Extract, Transform, Load)

    Входные параметры:
    sources_conn_params: str
        данные для подключения к исходной БД
    writer_conn_params: str
        данные для подключения к БД для записи
    conn_to_sources: Union[ConnectorToDds, ConnectorToSources]
        класс, в котором определены процессы Extract/Load для конкретной схемы
    conn_to_writer: Union[ConnectorToDds, ConnectorToDm],
        класс, в котором определены процессы Extract/Load для конкретной схемы
    transform_rules: Union[TransformSourcesToDds, TransformDdsToDm]
        класс, в котором определены правила трансформирования (Transform) данных

    Методы:
        extract_data_from_sources_db() -> dict
            Метод для реализации процесса Extract
        transform_data_from_sources_db(raw_data: dict) -> dict
            Метод для реализации процесса Transform
        load_data_to_writer_db(transformed_data: dict) -> None
            Метод для реализации процесса Load

    Примечание:
    Входные данные для подключения должны быть представлены в формате URI:
        'postgresql://<username>:<password>@<host>:<port>/<database>'
    """

    def __init__(
            self,
            sources_conn_params: str,
            writer_conn_params: str,
            conn_to_sources: Union[ConnectorToDds, ConnectorToSources],
            conn_to_writer: Union[ConnectorToDds, ConnectorToDm],
            transform_rules: Union[TransformSourcesToDds, TransformDdsToDm]
        ) -> None:
        
        self.__sources_conn_params = sources_conn_params
        self.__writer_conn_params = writer_conn_params
        self.__conn_to_sources = conn_to_sources
        self.__conn_to_writer = conn_to_writer
        self.__transform_rules = transform_rules


    def extract_data_from_sources_db(self) -> dict:
        """
        Метод для извлечения (Extract) данных из исходной БД.

        Returns:
        Словарь с данными таблиц исходной БД.
        """
        
        # Инициализируем переменную raw_data, чтобы хранить данные исходной БД.
        raw_data = None

        # Создаем объект и используем менеджер контекста with для обеспечения 
        # автоматического закрытия соединения после использования.
        with self.__conn_to_sources(self.__sources_conn_params) as sources:

            # Используем метод get_raw_data(), чтобы получить данные таблиц исходной БД.
            raw_data = sources.get_raw_data()

        # Возвращаем полученные данные таблиц исходной БД.
        return raw_data


    def transform_data_from_sources_db(self, raw_data: dict) -> dict:
        """
        Метод для преобразования (Transform) данных из исходной БД.

        Входные параметры:
        raw_data: dict
            Словарь с данными таблиц исходной БД.

        Returns:
        Словарь с преобразованными данными для дальнейшей записи.
        """
        
        # Инициализируем переменную, чтобы хранить преобразованные данные.
        transform_obj = self.__transform_rules(self.__writer_conn_params)

        # Запускаем функцию преобразования данных по правилу из self.__transform_rules
        transformed_data = transform_obj.get_transformed_data(raw_data)

        # Возвращаем трансформированные данные
        return transformed_data
    

    def load_data_to_writer_db(self, transformed_data: dict) -> None:
        """
        Метод для загрузки (Load) преобразованных данных

        Входные параметры:
        transformed_data: dict
            Словарь с преобразованными данными для таблиц.
        """

        # Создаем объект и используем менеджер контекста with для обеспечения 
        # автоматического закрытия соединения после использования.
        with self.__conn_to_writer(self.__writer_conn_params) as writer:

            # Используем метод load_transformed_data() для загрузки преобразованных 
            # данных (transformed_data).
            writer.load_transformed_data(transformed_data)
