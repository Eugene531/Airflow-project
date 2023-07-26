import io
import pandas as pd
from sqlalchemy import Engine, pool, create_engine


class TransformRulesDdsToDm:
    """
    """

    def __init__(self, db_conn_params: str, sources_data: dict=None) -> None:
        self.__db_conn_params: str = db_conn_params
        self.__sources_data: dict = sources_data

        self.__schema: str = 'dds'  # Имя основной схемы БД "dds"
        self.__temp_schema: str = 'temp_dds'  # Имя временной схемы БД "temp_dds"

        # Таблицы для перезаписи
        self.__tables_to_overwrite: list = [
            'brand',               # Таблица с данными о брендах
            'category',            # Таблица с данными о категориях
            'product',             # Таблица с данными о продуктах
            'stock',               # Таблица с данными о складах
            'transaction',         # Таблица с данными о транзакциях
        ]

        # Таблицы с ошибками
        self.__error_tables: list = [
            'product_errors',       # Таблица с ошибками в данных о продуктах
            'stock_errors',         # Таблица с ошибками в данных о складах
            'transaction_errors',   # Таблица с ошибками в данных о транзакциях
        ]

        # Неизменяемые таблицы
        self.__static_tables: list = [
            'tran_shop',           # Таблица с данными о магазинах транзакций
            'shop',                # Таблица с данными о магазинах
        ]

        # Значения по умолчанию таблиц для перезаписи
        self.__brand: pd.DataFrame = pd.DataFrame()
        self.__category: pd.DataFrame = pd.DataFrame()
        self.__product: pd.DataFrame = pd.DataFrame()
        self.__stock: pd.DataFrame = pd.DataFrame()
        self.__transaction: pd.DataFrame = pd.DataFrame()

        # Значения по умолчанию таблиц с ошибками для перезаписи
        self.__product_errors: pd.DataFrame = pd.DataFrame()
        self.__stock_errors: pd.DataFrame = pd.DataFrame()
        self.__transaction_errors: pd.DataFrame = pd.DataFrame()

        # Значения по умолчанию для неизменяемых таблиц
        self.__tran_shop: pd.DataFrame = pd.DataFrame()
        self.__shop: pd.DataFrame = pd.DataFrame()


    def __enter__(self) -> 'ConnectorToDdsSchema':
        """
        Метод для входа в контекст класса 'ConnectorToDdsSchema'.

        Returns
        Экземпляр класса 'ConnectorToDdsSchema'
        """

        # Создаем объект сессии (engine) для подключения к БД.
        self.__engine: Engine = create_engine(self.__db_conn_params)

        # Получаем подключение (connection) из объекта сессии (engine)
        self.__conn: pool.base._ConnectionFairy = self.__engine.raw_connection()

        # Возвращаем экземпляр класса для его использования в блоке 'with'
        return self
    

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Метод для выхода из контекста класса 'ConnectorToDdsSchema'.

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


    def get_transformed_data(self) -> dict:
        """
        Метод для получения данных из таблиц для перезаписи схемы 'dds' после очистки.

        Returns:
        Словарь, где ключами служат имена таблиц, а значениями - DataFrame с 
        данными соответствующих таблиц.
        """
        
        # Определяем данные таблиц для перезаписи (при помощи setattr)
        self.__set_overwrite_tables_data()

        # Определяем данные неизменяемых таблиц (при помощи setattr)
        self.__set_static_tables_data()

        # Запуск методов для преобразования данных (для каждой из таблиц свой метод)
        self.__transform_brand_data()
        self.__transform_category_data()
        self.__transform_product_data()
        self.__transform_stock_data()
        self.__transform_transaction_data()

        # Возвращаем приватные атрибуты - значения таблиц для перезаписи
        private = lambda n: f'_{self.__class__.__name__}__{n}'
        
        # Получаем словарь названий таблиц для перезаписи и их значений
        data = {tab: getattr(self, private(tab)) for tab in self.__tables_to_overwrite}
        
        # Добавляем в словарь данные о таблицах с ошибками
        data.update({tab: getattr(self, private(tab)) for tab in self.__error_tables})

        return data
    

    def __set_overwrite_tables_data(self) -> None:
        """
        Метод для определения данных в таблицах для перезаписи.
        Информация берется из исходной БД (self.__sources_data).

        Исключения:
        KeyError:
            названия таблиц для перезаписи не найдены в исходной БД (self.__sources_data).
        """

        # Проверяем, что данные из исходной БД (self.__sources_data) не равны None.
        # Если данных нет, то ничего не делаем, т.к. нет информации для перезаписи.
        if self.__sources_data is not None:

            # Итерируемся по таблицам для перезаписи (self.__tables_to_overwrite).
            for table_name in self.__tables_to_overwrite:

                # Пытаемся получить данные для текущей таблицы (table_name) из исходной БД.
                try:
                    data = self.__sources_data[table_name]
                except KeyError:
                    # Вывод предупреждения и продолжение выполнения программы без некоторых данных
                    print(f"Предупреждение: Данные для таблицы '{table_name}' не найдены.")
                    continue

                # Если данные для текущей таблицы были успешно получены из источника,
                # то устанавливаем их в соответствующий атрибут экземпляра класса.
                private = lambda n: f'_{self.__class__.__name__}__{n}'
                setattr(self, private(table_name), data)


    def __set_static_tables_data(self) -> None:
        """
        Метод для получения данных из схемы 'dds' для неизменяемых таблиц
        и сохранения их в приватных атрибутах объекта класса.
        """

        # Создаем объект сессии (Session) для работы с базой данных
        Session = sessionmaker(bind=self.__engine)

        # Начинаем транзакцию в базе данных
        with Session() as session:
            with session.begin():
                # Формируем SELECT SQL-запросы для каждой таблицы в списке __static_tables
                sqls = [f'SELECT * FROM {self.__schema}.{tab}' for tab in self.__static_tables]

                # Выполняем SQL-запросы и получаем данные таблиц в формате pandas DataFrame
                tables_data = [pd.read_sql_query(sql, session.bind) for sql in sqls]

        # Сохраняем данные таблиц в приватных атрибутах класса
        for table_name, table_data in zip(self.__static_tables, tables_data):
            # Формируем имя приватного атрибута
            private_table_name = f'_{self.__class__.__name__}__{table_name}'

            # Сохраняем данные в атрибут класса с указанным именем
            setattr(self, private_table_name, table_data)

    
    def __transform_brand_data(self) -> None:
        """
        Преобразует данные в таблице 'brand'.
        """

        # Получаем все столбцы таблицы 'brand'
        columns = self.__brand.columns

        # Проверяем, что "brand_id" не является числом
        is_invalid_brand_id = ~self.__brand['brand_id'].str.match(r'^\d+$')

        # Проверяем, что "brand" является целым положительным числом
        is_positive_integer_brand = self.__brand['brand'].str.match(r'^\d+$')

        # Комбинируем два условия и меняем местами записи
        mask = is_invalid_brand_id & is_positive_integer_brand
        self.__brand.loc[mask, columns] = \
            self.__brand.loc[mask, columns].values[0][::-1]

        # Удаляем повторяющиеся значения поля "brand_id"
        self.__brand.drop_duplicates(subset='brand_id', keep='first', inplace=True)


    def __transform_category_data(self) -> None:
        """
        Преобразует данные в таблице 'category'.
        """

        # Удаляем дубликаты по первичному ключу 'category_id'
        self.__category.drop_duplicates(
            subset=['category_id'], keep='first', inplace=True
            )


    def __transform_product_data(self) -> None:
        """
        Преобразует данные в таблице 'product'.
        """
        
        # Заменяем значение 'О00' на 'O00' в столбце 'category_id'
        self.__product['category_id'] = self.__product['category_id'].replace(
            'О00', 'O00')

        # Проверка наличия категории в списке 'category' и замена неимеющихся на 'NC'
        category = set(self.__category['category_id'])
        mask = ~self.__product['category_id'].isin(category)
        self.__product.loc[mask, 'category_id'] = 'NC'

        # Удаляем дубликаты по первичному ключу 'product_id'
        self.__product.drop_duplicates(
            subset=['product_id'], keep='first', inplace=True)

        # Определяем значения таблицы с ошибками        
        self.__product_errors = self.__product.copy()
        self.__product_errors['name_short_error'] = ''

        # Проверяем, что "name_short" не является числом
        mask = self.__product['name_short'].str.match(r'^\d+$')
        self.__product = self.__product.loc[~mask]

        # Определяем ошибочные данные
        self.__product_errors.loc[mask, 'name_short_error'] = '+'


    def __transform_stock_data(self) -> None:
        """
        Преобразует данные в таблице 'stock'.
        """
        
        # Преобразуем столбец "available_quantity" в числовой тип
        self.__stock['available_quantity'] = pd.to_numeric(
            self.__stock['available_quantity'], errors='coerce'
            )

        # Определяем данные таблицы с ошибкой
        self.__stock_errors = self.__stock.copy()
        
        # Проверка на условия:
        # "product_id" - целое положительное число
        # "available_quantity" - положительное числа
        mask = (
            self.__stock['product_id'].str.isnumeric() & 
            self.__stock['available_quantity'].apply(
                lambda x: isinstance(x, (float, int)) and x > 0
                )
            )
        
        # Разделение данных
        self.__stock = self.__stock.loc[mask]

        # Определяем значения таблицы с ошибками
        self.__stock_errors['product_id_error'] = ''
        self.__stock_errors['available_quantity_error'] = ''
        self.__stock_errors.loc[~mask,['product_id_error','available_quantity_error']]\
            = '+'

        # Убираем пустые значения в столбце 'cost_per_item'
        mask = self.__stock['cost_per_item'].apply(lambda x: x != '')
        self.__stock = self.__stock.loc[mask]

        # Определяем значения таблицы с ошибками
        mask = self.__stock_errors['cost_per_item'].apply(lambda x: x != '')
        self.__stock_errors['cost_per_item_error'] = ''
        self.__stock_errors.loc[~mask, 'cost_per_item_error'] = '+'

        # Убираем значения из stock, которых нет в product, по product_id
        product = set(self.__product['product_id'])
        mask = self.__stock['product_id'].isin(product)
        self.__stock = self.__stock.loc[mask]

        # Определяем значения таблицы с ошибками
        mask = self.__stock_errors['product_id'].isin(product)
        self.__stock_errors.loc[~mask, 'product_id_error'] = '+'

        # Убираем значения из stock, которых нет в shop, по pos
        shop = set(self.__shop['pos'])
        mask = self.__stock['pos'].isin(shop)
        self.__stock = self.__stock.loc[mask]

        # Определяем значения таблицы с ошибками
        mask = self.__stock_errors['pos'].isin(shop)
        self.__stock_errors['pos_error'] = ''
        self.__stock_errors.loc[~mask, 'pos_error'] = '+'

        # Конвертация даты из строкового формата в числовой формат
        self.__stock['available_on'] = pd.to_numeric(
            self.__stock['available_on'], errors='coerce'
            )

        # Конвертация даты из числового формата в обычный формат даты
        self.__stock['available_on'] = pd.to_datetime(
            self.__stock['available_on'], origin='1899-12-30', unit='D'
            )

        # Удаляем дубликаты по первичному ключу ('available_on', 'product_id', 'pos')
        self.__stock.drop_duplicates(
            subset=['available_on', 'product_id', 'pos'], keep='first', inplace=True
            )
        
        # Для таблицы с ошибками
        self.__stock_errors.drop_duplicates(
            subset=['available_on', 'product_id', 'pos'], keep='first', inplace=True
            )


    def __transform_transaction_data(self) -> None:
        """
        Преобразует данные в таблице 'transaction'.
        """
        
        # Проверка на условия:
        # "quantity" - не пустое
        # "price" - не пустое
        mask = (
            self.__transaction['quantity'].apply(lambda x: x != '') &
            self.__transaction['price'].apply(lambda x: x != '')
            )
        
        # Определяем значения таблицы с ошибками
        self.__transaction_errors = self.__transaction.copy()
        
        # Убираем пустые значения
        self.__transaction = self.__transaction[mask]

        # Определяем значения таблицы с ошибками
        mask1 = self.__transaction_errors['quantity'].apply(lambda x: x == '')
        mask2 = self.__transaction_errors['price'].apply(lambda x: x == '')
        
        self.__transaction_errors['quantity_error'] = ''
        self.__transaction_errors['price_error'] = ''
        self.__transaction_errors.loc[mask1, 'quantity_error'] = '+'
        self.__transaction_errors.loc[mask2, 'price_error'] = '+'

        # Убираем значения из transaction, которых нет в product, по product_id
        product = set(self.__product['product_id'])
        mask = self.__transaction['product_id'].isin(product)
        self.__transaction = self.__transaction.loc[mask]

        # Определяем значения таблицы с ошибками
        mask = self.__transaction_errors['product_id'].isin(product)
        self.__transaction_errors['product_id_error'] = ''
        self.__transaction_errors.loc[~mask, 'product_id_error'] = '+'

        # Убираем значения из transaction, которых нет в tran_shop, по transaction_id
        tran_shop = set(self.__tran_shop['transaction_id'])
        mask = self.__transaction['transaction_id'].isin(tran_shop)
        self.__transaction = self.__transaction.loc[mask]

        # Определяем значения таблицы с ошибками
        mask = self.__transaction_errors['transaction_id'].isin(tran_shop)
        self.__transaction_errors['transaction_id_error'] = ''
        self.__transaction_errors.loc[~mask, 'transaction_id_error'] = '+'

        # Удаляем дубликаты по первичному ключу 'transaction_id'
        self.__transaction.drop_duplicates(
            subset=['transaction_id', 'product_id'], keep='first', inplace=True
            )
        
        # Для таблицы с ошибками
        self.__transaction_errors.drop_duplicates(
            subset=['transaction_id', 'product_id'], keep='first', inplace=True
            )
        
    
    def load_transformed_data(self, transformed_data: dict) -> None:
        """
        Метод для загрузки преобразованных данных в схему 'dds'.

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
        self.__delete_prev_data()


    def __switch_schems(self) -> None:
        """
        Метод для переключения схем 'dds' и 'temp_dds' в БД.
        """

        with self.__conn.cursor() as cursor:
            # Переименовываем временную схему 'temp_dds' в 'temp'
            cursor.execute('ALTER SCHEMA temp_dds RENAME TO temp;')

            # Переименовываем основную схему 'dds' во временную 'temp_dds'
            cursor.execute('ALTER SCHEMA dds RENAME TO temp_dds;')

            # Переименовываем временную схему 'temp' в основную 'dds'
            cursor.execute('ALTER SCHEMA temp RENAME TO dds;')

        
    def __delete_prev_data(self) -> None:
        """
        Метод для удаления предыдущих данных в схеме 'temp_dds'.
        """

        with self.__conn.cursor() as cursor:
            # Устанавливаем схему для текущей сессии
            cursor.execute(f'SET search_path TO {self.__temp_schema}')

            # Удаляем предыдущие данные из каждой таблицы для перезаписи
            for table_name in self.__tables_to_overwrite:
                cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")

            # Удаляем предыдущие данные из каждой таблицы с ошибками
            for table_name in self.__error_tables:
                cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
