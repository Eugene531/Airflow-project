from datetime import datetime
import io
import pandas as pd
from sqlalchemy import Engine, pool, create_engine
from sqlalchemy.orm import sessionmaker


class TransformSourcesToDds:
    """
    """

    def __init__(self, , raw_data: dict) -> None:
        self.__raw: str = db_conn_params

        self.__schema: str = 'dm'  # Имя основной схемы БД "dm"

        # Необходимы для расчетов таблицы из dds
        self.__tables_from_dds: list = [
            'brand',               # Таблица с данными о брендах
            'category',            # Таблица с данными о категориях
            'product',             # Таблица с данными о продуктах
            'stock',               # Таблица с данными о складах
            'transaction',         # Таблица с данными о транзакциях
            'shop',
            'tran_shop',
        ]


    def __enter__(self) -> 'ConnectorToDatamartsSchema':
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


    def get_data_from_dds(self) -> dict:
        """
        Метод для получения данных из схемы 'dds' и сохранения их в приватных
        атрибутах объекта класса.
        """

        # Создаем объект сессии (Session) для работы с базой данных
        Session = sessionmaker(bind=self.__engine)

        # Начинаем транзакцию в базе данных
        with Session() as session:
            with session.begin():
                # Формируем SELECT SQL-запросы для каждой таблицы в списке __static_tables
                sqls = [f'SELECT * FROM dds.{tab}' for tab in self.__tables_from_dds]

                # Выполняем SQL-запросы и получаем данные таблиц в формате pandas DataFrame
                tables_data = [pd.read_sql_query(sql, session.bind) for sql in sqls]

        return dict(zip(self.__tables_from_dds, tables_data))
    

    def get_transformed_data(self, sources_data: dict):
        product = sources_data['product']
        category = sources_data['category']
        tran_shop = sources_data['tran_shop']
        shop = sources_data['shop']
        transaction = sources_data['transaction']
        brand = sources_data['brand']
        stock = sources_data['stock']

        # # Присоединение таблиц с использованием операций left join
        # merged_data = (
        #     transaction
        #     .merge(tran_shop, on='transaction_id', how='left')
        #     .merge(product, on='product_id', how='left')
        #     .merge(brand, on='brand_id', how='left')
        #     .merge(category, on='category_id', how='left')
        # )

        # # Добавление столбца 'day' с помощью функции EXTRACT
        # merged_data['day'] = merged_data['recorded_on'].dt.dayofweek
        
        # # Преобразование столбца 'recorded_on' в формат 'YYYY-MM'
        # merged_data['date'] = merged_data['recorded_on'].dt.strftime('%Y-%m')

        # # Выполнение агрегации данных
        # aggregated_data = (
        #     merged_data
        #     .groupby(['date', 'day', 'pos', 'category_name', 'name_short'])
        #     .agg(sum_price=('price', 'sum'), sum_quantity=('quantity', 'sum'))
        #     .reset_index()
        # )

        # # Сортировка результатов
        # sorted_data = (
        #     aggregated_data
        #     .sort_values(by=['date', 'pos', 'category_name', 'name_short', 'sum_price', 'sum_quantity'])
        # )

        # # Добавление столбца "UPDATE_DATE" с текущей датой и временем
        # sorted_data['UPDATE_DATE'] = datetime.now()

        

        ##############################################################################################




        # transaction_data = transaction.copy()
        
        # # Преобразование столбца "recorded_on" в формат 'YYYY-MM'
        # transaction_data['date'] = pd.to_datetime(transaction_data['recorded_on']).dt.strftime('%Y-%m')

        # # Создание вспомогательного DataFrame subq
        # subq = transaction_data.merge(tran_shop, on='transaction_id', how='left')
        # subq['total'] = subq.groupby('transaction_id')['price'].transform('sum')

        # # Группировка и вычисление среднего значения total для каждой даты и pos
        # result_data = subq.groupby(['date', 'pos'])['total'].mean().reset_index()

        # # Добавление столбца "UPDATE_DATE" с текущей датой и временем
        # result_data['UPDATE_DATE'] = datetime.now()




        ##############################################################################################




        # stock_data = stock.copy()

        # # Группируем данные по 'product_id' и 'pos', выбираем максимальную дату ('available_on') и суммируем 'available_quantity',
        # # также используем 'cost_per_item' в качестве агрегирующей функции (он не меняется при агрегации)
        # result = stock_data.groupby(['product_id']).agg({
        #     'available_on': 'max',
        #     'available_quantity': 'sum',
        #     'cost_per_item': 'first'
        # }).reset_index()

        # # Объединяем таблицы product и category по столбцу 'category_id' с помощью left join
        # result = (
        #     result
        #     .merge(product[['product_id', 'name_short', 'category_id']], on='product_id', how='left')
        #     .merge(category[['category_id', 'category_name']], on='category_id', how='left')
        # )

        # # Удаляем поле 'category_id' из результата
        # result.drop(columns=['category_id'], inplace=True)

        # # Обработка таблицы transactions
        # transaction_data = transaction.copy()

        # transaction_data['recorded_on'] = pd.to_datetime(transaction_data['recorded_on']).dt.strftime('%Y-%m')

        # # Отбираем только те строки, где 'order_type_id' = 'BUY'
        # filtered_transactions = transaction_data[transaction_data['order_type_id'] == 'BUY']

        # # Группируем данные по 'product_id' и 'month', суммируем 'quantity'
        # grouped_transactions = filtered_transactions.groupby(['product_id', 'recorded_on']).agg({
        #     'quantity': 'sum'
        # }).reset_index()
        
        # # Объединяем result с grouped_transactions по столбцу 'product_id' с помощью left join
        # result = result.merge(grouped_transactions, on='product_id', how='left')

        # result.drop(columns=['recorded_on'], inplace=True)
        
        # # Добавление столбца "UPDATE_DATE" с текущей датой и временем
        # result['UPDATE_DATE'] = datetime.now()

        # # Замена значений NaN на 0
        # result['quantity'] = result['quantity'].fillna(0)

        # # Переименовываем поле 'available_quantity' в 'stock_total'
        # result.rename(columns={'available_quantity': 'stock_total'}, inplace=True)

        # # Переименовываем поле 'available_on' в 'update_date_in_stock'
        # result.rename(columns={'available_on': 'update_date_in_stock'}, inplace=True)
        
        # # Переименовываем поле 'quantity' в 'sold_total'
        # result.rename(columns={'quantity': 'sold_total'}, inplace=True)

        # # Переименовываем поле 'category_name' в 'category'
        # result.rename(columns={'category_name': 'category'}, inplace=True)

        




        ##############################################################################################





        # transaction_data = transaction.copy()
        
        # transaction_data['date'] = transaction_data['recorded_on'].dt.strftime('%Y-%m')
        
        # # Отбираем только те строки, где 'order_type_id' = 'BUY'
        # filtered_transactions = transaction_data[transaction_data['order_type_id'] == 'BUY']

        # # Группируем данные по 'product_id' и 'date' и вычисляем среднее значение кол-ва проданных продуктов и цену
        # average_quantity_per_month = filtered_transactions.groupby(['product_id', 'date'])[['quantity', 'price']].mean().reset_index()

        # # Создаем все комбинации 'product_id' и 'date'
        # all_product_month_combinations = pd.MultiIndex.from_product([
        #     product['product_id'].unique(), transaction_data['date'].unique()], names=['product_id', 'date'])

        # # Создаем DataFrame с комбинациями и объединяем его с результатом агрегации по 'product_id' и 'month' с помощью right join
        # merged_data = all_product_month_combinations.to_frame(index=False).merge(average_quantity_per_month, on=['product_id', 'date'], how='left')

        # # Переименовываем поле 'price' в 'avg_price'
        # merged_data.rename(columns={'price': 'avg_price'}, inplace=True)

        # # Заполняем NaN значения в столбце 'quantity' нулями
        # merged_data['quantity'] = merged_data['quantity'].fillna(0)

        # # Заполняем NaN значения в столбце 'price' нулями
        # merged_data['avg_price'] = merged_data['avg_price'].fillna(0)

        # # Добавление столбца "UPDATE_DATE" с текущей датой и временем
        # merged_data['UPDATE_DATE'] = datetime.now()

        
        

        
        ##############################################################################################




        transaction_data = transaction.copy()

        transaction_data['date'] = transaction_data['recorded_on'].dt.strftime('%Y-%m')
        
        filtered_transactions = transaction_data[transaction_data['order_type_id'] == 'BUY']

        # Создаем новый столбец 'total_gross', в котором хранится произведение 'quantity' на 'price'
        filtered_transactions['total_gross'] = filtered_transactions['quantity'] * filtered_transactions['price']

        average_quantity_per_month = filtered_transactions.groupby(['date']).agg({
            'total_gross': 'sum',
            'quantity': 'sum'
        }).reset_index()

        # Получаем количество уникальных значений 'pos' из таблицы 'shop'
        unique_pos_count = shop['pos'].nunique()

        # Вычисляем средний чек ('avg_check') как total_gross / количество уникальных значений 'pos'
        average_quantity_per_month['avg_check'] = average_quantity_per_month['total_gross'] / unique_pos_count

        # Переименовываем поле 'quantity' в 'total_volume'
        average_quantity_per_month.rename(columns={'quantity': 'total_volume'}, inplace=True)

        # Добавление столбца "UPDATE_DATE" с текущей датой и временем
        average_quantity_per_month['UPDATE_DATE'] = datetime.now()


        data = {
            # 'transactions_group_ymd': sorted_data
            #'average_check': result_data
        }

        print(average_quantity_per_month)
        
        self.load_transformed_data({'total_stats': average_quantity_per_month})

        return 

    
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
            cursor.execute(f'SET search_path TO {self.__schema}')

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
        
        return
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
