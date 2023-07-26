import io
import pandas as pd
from sqlalchemy import pool, create_engine
from sqlalchemy.orm import sessionmaker


class ConnectorToDds:
    """
    Используется для подключения к БД и выгрузки (Extract), загрузки (Load)
    данных в контексте схемы 'dds'.

    Для взаимодействия с БД используется менеджер контекстов, например:
        with ConnectorToDdsSchema(db_conn_params) as conn

    Входные параметры:
        db_conn_params: str
            Параметры подключения к БД для записи.

    Методы:
        get_raw_data() -> dict:
            Метод для получения данных из таблиц схемы 'dds'.
        load_transformed_data(transformed_data: dict) -> None:
            Метод для загрузки данных в схему 'dds'.
    """

    def __init__(self, db_conn_params: str) -> None:
        self.__db_conn_params: str = db_conn_params

        self.__schema: str = 'dds'  # Имя основной схемы БД "dds"
        self.__temp_schema: str = 'temp_dds'  # Имя временной схемы БД "temp_dds"

        # Таблицы для возврата при вызове метода 'get_raw_data'
        self.__tables_to_return: list = [
            'brand',               # Таблица с данными о брендах
            'category',            # Таблица с данными о категориях
            'product',             # Таблица с данными о продуктах
            'stock',               # Таблица с данными о складах
            'transaction',         # Таблица с данными о транзакциях
            'tran_shop',           # Таблица с данными о магазинах транзакций
            'shop',                # Таблица с данными о магазинах
        ]


    def __enter__(self) -> 'ConnectorToDds':
        """
        Метод для входа в контекст класса 'ConnectorToDds'.

        Returns
        Экземпляр класса 'ConnectorToDds'
        """

        # Создаем объект сессии (engine) для подключения к БД.
        self.__engine = create_engine(self.__db_conn_params)

        # Получаем подключение (connection) из объекта сессии (engine)
        self.__conn: pool.base._ConnectionFairy = self.__engine.raw_connection()

        # Возвращаем экземпляр класса для его использования в блоке 'with'
        return self
    

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Метод для выхода из контекста класса 'ConnectorToDds'.

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

        # Закрываем подключение к базе данных, чтобы освободить ресурсы.
        self.__conn.close()


    def get_raw_data(self) -> dict:
        """
        Метод для получения данных из схемы 'dds'.
        """

        # Создаем объект сессии (Session) для работы с БД
        Session = sessionmaker(bind=self.__engine)

        # Начинаем транзакцию в базе данных
        with Session() as session:
            with session.begin():
                # Формируем SELECT SQL-запросы для каждой таблицы в списке __tables_to_return
                sqls = [f'SELECT * FROM {self.__schema}.{tab}' for tab in \
                        self.__tables_to_return]

                # Выполняем SQL-запросы и получаем данные таблиц в формате pandas DataFrame
                tables_data = [pd.read_sql_query(sql, session.bind) for sql in sqls]

        # Возвращаем данные
        return dict(zip(self.__tables_to_return, tables_data))
        
    
    def load_transformed_data(self, transformed_data: dict) -> None:
        """
        Метод для загрузки данных в схему 'dds'.

        Входные параметры:
        transformed_data: dict
            Словарь с преобразованными данными, где ключ - имя таблицы, а значение - 
            pandas DataFrame с данными для этой таблицы.
        """

        with self.__conn.cursor() as cursor:
            # Устанавливаем схему для текущей сессии
            cursor.execute(f'SET search_path TO {self.__temp_schema}')

            # Размер копируемых данных (10 МБ)
            copy_size = 10 * 1024 * 1024

            # Проходим по всем таблицам и соответствующим данным в словаре transformed_data
            for table, df in transformed_data.items():
                # Преобразуем DataFrame в CSV-формат и создаем временный буфер
                buffer = io.StringIO()
                df.to_csv(buffer, sep='\t', header=False, index=False, na_rep='\\N')
                buffer.seek(0)
                
                # Формируем запрос INSERT для вставки данных из временного буфера
                cursor.copy_from(buffer, table, sep='\t', null='\\N', size=copy_size)
        
        # Переключаем схемы в базе данных
        self.__switch_schems()

        # Удаляем предыдущие данные
        self.__delete_prev_data(list(transformed_data.keys()))


    def __switch_schems(self) -> None:
        """
        Метод для переключения схем 'dds' и 'temp_dds' в БД.
        """

        with self.__conn.cursor() as cursor:
            # Переименовываем временную схему в 'temp'
            cursor.execute(f'ALTER SCHEMA {self.__temp_schema} RENAME TO temp;')

            # Переименовываем основную схему во временную
            cursor.execute(f'ALTER SCHEMA {self.__schema} RENAME TO {self.__temp_schema};')

            # Переименовываем временную схему 'temp' в основную
            cursor.execute(f'ALTER SCHEMA temp RENAME TO {self.__schema};')

        
    def __delete_prev_data(self, tables_name: list) -> None:
        """
        Метод для удаления предыдущих данных во временной схеме.
        """

        with self.__conn.cursor() as cursor:
            # Устанавливаем схему для текущей сессии
            cursor.execute(f'SET search_path TO {self.__temp_schema}')

            # Удаляем предыдущие данные из каждой таблицы
            for table_name in tables_name:
                cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
