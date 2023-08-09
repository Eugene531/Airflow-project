import io
import os
import pandas as pd
from sqlalchemy import pool, create_engine
from sqlalchemy.orm import sessionmaker


class Connector:
    """
    Используется для извлечения (Exctract) и загрузки (Load) данных из/в БД.

    Для взаимодействия с БД необходимо использовать менеджер контекстов, например:
        with Connector(db_config, tables) as Connection

    Входные параметры:
        **kwargs: dict
            Словарь, в котором могут быть определены следующие параметры:
                config: str
                    Параметры подключения к БД в формате URI.
                tables: list
                    Список таблиц для выгрузки/загрузки данных.
                schema: str
                    Схема БД для подключения. Изначально public.

    Методы:
        extract_data() -> dict:
            Метод для получения данных из указанных таблиц схемы БД.
        load_transformed_data(transformed_data: dict) -> None:
            Метод для загрузки данных в указанную схему БД.
    """

    def __init__(self, **kwargs) -> None:
        self.__db_config: str = kwargs['config'] # Параемтры подключения к БД  (URI)
        self.__tables: list = kwargs['tables'] # Таблицы для выгрузки/загрузки данных
        self.__schema: str = 'public' # Схема БД для подключения. Изначально public

        # Определяем схему, если была передана
        if 'schema' in kwargs:
            self.__schema = kwargs['schema']


    def __enter__(self) -> 'Connector':
        """
        Метод для входа в контекст класса 'Connector'.
        """

        # Создаем объект сессии (engine) для подключения к БД
        self.__engine = create_engine(self.__db_config)

        # Получаем подключение (connection) из объекта сессии (engine)
        self.__conn: pool.base._ConnectionFairy = self.__engine.raw_connection()

        # Возвращаем экземпляр класса для его использования в блоке 'with'
        return self
    

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Метод для выхода из контекста класса 'Connector'.

        Входные параметры:
        exc_type:
            Тип возникшего исключения (если оно есть).
        exc_value:
            Значение возникшего исключения (если оно есть).
        traceback:
            Информация о трассировке стека при возникновении исключения (если оно есть).
        """

        # Если возникла ошибка (exc_type не равен None), то выполняем откат (rollback).
        if exc_type is not None:
            self.__conn.rollback()
        else:
            # Если ошибки нет (exc_type равен None), то выполняем фиксацию (commit).
            self.__conn.commit()

        # Закрываем подключение к БД, чтобы освободить ресурсы.
        self.__conn.close()


    def extract_data(self) -> dict:
        """
        Метод для Извлечение данных из таблиц.

        Returns:
        Словарь с данными, где ключи - имена таблиц, значения - DataFrames.
        """

        # Создание сессии для подключения к БД
        Session = sessionmaker(bind=self.__engine)

        # Начало транзакции в БД
        with Session() as session:
            with session.begin():
                # Формирование SQL-запросов SELECT для каждой таблицы
                selects = [f'SELECT * FROM {self.__schema}.{table}' for table in self.__tables]

                # Выполнение SQL-запросов и получение данных в формате pandas DataFrame
                tables_data = [pd.read_sql_query(query, session.bind) for query in selects]

        # Возврат данных в виде словаря
        return dict(zip(self.__tables, tables_data))
        
    
    def load_data(self, data: dict) -> None:
        """
        Метод для загрузка данных в таблицы.

        Входные параметры:
        data: dict
            Словарь с данными, где ключи - имена таблиц, значения - DataFrames.
        """

        # Удаление старой временной схемы, если она существует.
        self.__delete_old_temp_schem()

        # Создание новой временной схемы.
        self.__create_new_temp_schem()

        # Загрузка данных во временную схему.
        self.__load_data_to_temp(data)

        # Переключение схем для актуализации данных.
        self.__switch_schems()

        # Удаление старой временной схемы.
        self.__delete_old_temp_schem()


    def __delete_old_temp_schem(self) -> None:
        """
        Метод для удаление старой временной схемы, если она существует.
        """

        # Выполняем SQL запрос удаления схемы при помощи курсора "cursor"
        with self.__conn.cursor() as cursor:
            cursor.execute(f'drop SCHEMA IF EXISTS temp_{self.__schema} cascade')


    def __create_new_temp_schem(self) -> None:
        """
        Метод для создания новой временной схемы.
        """

        # Определяем путь к SQL-скрипту для создания временной схемы
        file_path = os.path.join(
            os.path.dirname(__file__), 
            '..', '..', '..', '..', 
            'ddl', f'temp_{self.__schema}_create.sql'
            )

        # Чтение SQL-запросов из файла
        with open(file_path, 'r') as f:
            queries = f.read()

        # Используем курсор для выполнения SQL-запросов создания схемы
        with self.__conn.cursor() as cursor:
            cursor.execute(queries)

    
    def __load_data_to_temp(self, data: dict):
        """
        Метод для загрузки данных во временную схему.

        Входные параметры:
        data: dict
            Словарь с данными, где ключи - имена таблиц, значения - DataFrames.
        """

        # Используем курсор для выполнения SQL-запросов
        with self.__conn.cursor() as cursor:
            # Устанавливаем временную схему для текущей сессии
            cursor.execute(f'SET search_path TO temp_{self.__schema}')

            # Размер копируемых данных (10 МБ)
            copy_size = 10 * 1024 * 1024

            # Проходим по всем таблицам и соответствующим данным в словаре "data"
            for table, df in data.items():
                # Преобразуем DataFrame в CSV-формат и создаем временный буфер
                buffer = io.StringIO()
                df.to_csv(buffer, sep='\t', header=False, index=False, na_rep='\\N')
                buffer.seek(0)
                
                # Формируем запрос INSERT для вставки данных из временного буфера
                cursor.copy_from(buffer, table, sep='\t', null='\\N', size=copy_size)
    
    
    def __switch_schems(self) -> None:
        """
        Метод для переключения схем для актуализации данных.
        """

        # Используем курсор для выполнения SQL-запросов
        with self.__conn.cursor() as cursor:
            # Переименовываем основную схему в схему со старыми данными "old"
            cursor.execute(f'ALTER SCHEMA {self.__schema} RENAME TO old_{self.__schema};')

            # Переименовываем временную "temp" схему в основную
            cursor.execute(f'ALTER SCHEMA temp_{self.__schema} RENAME TO {self.__schema};')
