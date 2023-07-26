import pandas as pd
from sqlalchemy import Engine, pool, create_engine
from sqlalchemy.orm import sessionmaker


class ConnectorToSources:
    """
    Используется для подключения к БД и выгрузки данных в контексте схемы 'sources'

    Для взаимодействия с БД используется менеджер контекстов, например:
        with ConnectorToSourcesSchema(db_conn_params) as conn

    Входные параметры:
    db_conn_params: str
        Параметры подключения к исходной БД для чтения информации.

    Методы:
        get_transformed_data() -> dict
            Метод для получения данных таблиц исходной БД в схеме 'sources'.
    """
    
    def __init__(self, db_conn_params: str) -> None:
        self.__db_conn_params: str = db_conn_params
        
        self.__schema: str = 'sources' # Имя схемы БД

        # Таблицы схемы 'sources'
        self.__tables: list = [
            'brand',
            'category',
            'product',
            'stock',
            'transaction',
            ]
        
    
    def __enter__(self) -> 'ConnectorToSourcesSchema':
        """
        Метод для входа в контекст класса 'ConnectorToSourcesSchema'.

        Returns
        Экземпляр класса 'ConnectorToSourcesSchema'
        """

        # Создаем объект сессии (engine) для подключения к БД.
        self.__engine: Engine = create_engine(self.__db_conn_params)

        # Получаем подключение (connection) из объекта сессии (engine)
        self.__conn: pool.base._ConnectionFairy = self.__engine.raw_connection()

        # Возвращаем экземпляр класса для его использования в блоке 'with'
        return self
    

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Метод для выхода из контекста класса 'ConnectorToSourcesSchema'.

        Входные параметры:
        exc_type:
            Тип возникшего исключения (если оно есть).
        exc_value:
            Значение возникшего исключения (если оно есть).
        traceback:
            Информация о трассировке стека при возникновении исключения (если оно есть).
        """

        # Закрываем подключение к базе данных, чтобы освободить ресурсы.
        self.__conn.close()
        
    
    def get_tables_data(self) -> dict:
        """
        Метод для получения данных таблиц схемы 'sources'.

        Returns:
        Словарь, где ключами служат имена таблиц, а значениями - DataFrame с 
        данными соответствующих таблиц.
        """

        Session = sessionmaker(bind=self.__engine)

        with Session() as session:
            with session.begin():
                select_sqls = [f'SELECT * FROM {self.__schema}.{tab}' for tab in self.__tables]
                tables_data = [pd.read_sql_query(sql, session.bind) for sql in select_sqls]
        
        return dict(zip(self.__tables, tables_data))
