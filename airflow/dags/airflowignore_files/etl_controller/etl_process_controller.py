from ..connector_to_db.connector import Connector
from ..transform_rules.main_transform_rules import TransformRules


class EtlProcessorController:
    """
    Контроллер для управления процессами ETL (Extract, Transform, Load).

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
    
    Методы:
        extract_data() -> dict:
            Извлекает сырые данные из исходной БД.
        transform_data(raw_data: dict) -> dict:
            Преобразует данные с использованием правил трансформации.
        load_data(transformed_data: dict):
            Загружает преобразованные данные в целевую БД.

    Примечание:
        Входные данные для подключения должны быть представлены в формате URI:
            'postgresql://<username>:<password>@<host>:<port>/<database>'
    """

    def __init__(self, read_db_data: dict, write_db_data: dict, module: str) -> None:
        self.__read_db_data = read_db_data
        self.__write_db_data = write_db_data
        self.__module = module


    def extract_data(self) -> dict:
        """
        Метод для извлечения (Extract) сырых данных.

        Returns:
        Словарь с данными, где ключи - имена таблиц, значения - DataFrames.
        """
        
        # Инициализируем переменную raw_data, чтобы хранить сырые данные.
        raw_data = None

        # Создаем объект Connector для извлечения данных
        ExtractConnection = Connector(**self.__read_db_data)

        # Используем менеджер контекста для подключения
        with ExtractConnection as Connection:
            # Извлекаем сырые данные
            raw_data = Connection.extract_data()

        # Возвращаем сырые данные
        return raw_data
    

    def transform_data(self, raw_data: dict) -> dict:
        """
        Метод для преобразования (Transform) сырых данных по правилам трансформации.

        Входные параметры:
            raw_data: dict
                Словарь с сырыми данными.

        Returns:
        Словарь с преобразованными данными для дальнейшей записи.
        """

        # Создаем объект TransformRules
        TransformDataRules = TransformRules(self.__module)

        # Преобразуем данные при помощи метода get_transformed_data
        transformed_data = TransformDataRules.get_transformed_data(raw_data)

        # Возвращаем преобразованные данные
        return transformed_data
    

    def load_data(self, transformed_data: dict) -> None:
        """
        Метод для загрузки (Load) преобразованных данных.

        Входные параметры:
            transformed_data: dict
                Словарь с преобразованными данными, где ключи - имена таблиц, 
                значения - DataFrames.
        """

        # Создаем объект Connector для загрузки данных
        LoadConnection = Connector(**self.__write_db_data)
        # Используем менеджер контекста для подключения
        with LoadConnection as Connection:
            # Загружаем преобразованные данные
            Connection.load_data(transformed_data)
