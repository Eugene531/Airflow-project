from io import StringIO
import pandas as pd
import psycopg2


class ETLProcessor:
    def __init__(self, sources_db_conn_params, writer_db_conn_params):
        """
        Конструктор класса ETLProcessor.
        
        :param sources_db_conn_params: Параметры подключения к исходной базе данных.
        :param writer_db_conn_params: Параметры подключения к базе данных для записи результатов.
        """
        self.sources_db_conn_params = sources_db_conn_params
        self.writer_db_conn_params = writer_db_conn_params
        
        # Таблицы, которые будут перезаписываться
        self.brand = None
        self.category = None
        self.product = None
        self.stock = None
        self.transaction = None
        
        # Таблицы, которые не будут перезаписываться
        self.tran_shop, self.shop = self.__get_data_from_writer_db()

        # Устанавливаем значения параметров из исходной таблицы
        self.__set_data_from_sources_db()


    def transform_raw_data(self):
        """
        Запускает преобразование данных для каждой таблицы.
        """
        try:
            # Запуск преобразования данных для каждой таблицы
            self.__transform_brand_data()
            self.__transform_category_data()
            self.__transform_product_data()
            self.__transform_stock_data()
            self.__transform_transaction_data()
        except Exception as e:
            print(f"Произошла ошибка при преобразовании данных: {e}")


    def load_correct_data(self):
        try:
            # Подключение к базе данных для записи
            with psycopg2.connect(**self.writer_db_conn_params) as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:
                    # Установка схемы для текущей сессии
                    cursor.execute(f'SET search_path TO dds')

                    # Очистка таблиц перед вставкой новых данных (нужны временные таблицы)
                    self.__delete_prev_data(cursor)

                    # Загрузка данных в таблицы brand, category, product, stock и transaction
                    dfs = [self.brand, self.category, self.product, self.stock, self.transaction]
                    tables = ['brand', 'category', 'product', 'stock', 'transaction']
                    for df, table_name in zip(dfs, tables):
                        # Создаем временный буфер для формирования данных для вставки
                        buffer = StringIO()
                        df.to_csv(buffer, sep='\t', header=False, index=False, na_rep='\\N')
                        buffer.seek(0)

                        # Формируем запрос INSERT для вставки данных из временного буфера
                        cursor.copy_from(buffer, table_name, sep='\t', null='\\N')
        except Exception as e:
            print(f"Error occurred: {e}")


    @staticmethod
    def __delete_prev_data(cursor):
        """
        Очищает таблицы от старых значений.
        """
        tables_to_clear = ['stock', 'transaction', 'product', 'brand', 'category']
        for table_name in tables_to_clear:
            cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE;")


    def __get_data_from_writer_db(self):
        """
        Получает данные таблиц 'tran_shop' и 'shop' из базы данных для записи.
        
        :return: Данные для таблиц 'tran_shop' и 'shop'.
        """
        try:
            # Подключение к базе данных для записи
            with psycopg2.connect(**self.writer_db_conn_params) as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:
                    # Установка схемы для текущей сессии
                    cursor.execute(f'SET search_path TO dds')

                    # Формирование SQL-запросов для таблиц 'tran_shop' и 'shop'
                    tran_shop_sql = f'SELECT * FROM tran_shop'
                    shop_sql = f'SELECT * FROM shop'

                    # Извлечение данных из таблиц 'tran_shop' и 'shop'
                    tran_shop_data = pd.read_sql(tran_shop_sql, conn)
                    shop_data = pd.read_sql(shop_sql, conn)

            return tran_shop_data, shop_data
        except Exception as e:
            print(f"Произошла ошибка при извлечении данных из БД для записи: {e}")

    
    def __set_data_from_sources_db(self):
        """
        Получает данные из исходной БД и сохраняет их в соответствующие атрибуты класса.
        """
        try:
            # Установка схемы и таблиц, данные из которых будут извлечены
            schema = 'sources'
            tables = ['brand', 'category', 'product', 'stock', 'transaction']

            # Подключение к исходной базе данных
            with psycopg2.connect(**self.sources_db_conn_params) as conn:
                with conn.cursor() as cursor:
                    # Установка схемы для текущей сессии
                    cursor.execute(f'SET search_path TO {schema}')

                    # Извлечение данных из каждой таблицы и сохранение в соответствующий атрибут класса
                    for table in tables:
                        select_sql = f'SELECT * FROM {table}'
                        data = pd.read_sql(select_sql, conn)
                        setattr(self, table, data)
        except Exception as e:
            print(f"Произошла ошибка при извлечении данных из исходной БД: {e}")


    def __transform_brand_data(self):
        """
        Преобразует данные в таблице 'brand'.
        """
        columns = self.brand.columns

        # Проверяем, что "brand_id" не является числом
        is_invalid_brand_id = ~self.brand['brand_id'].str.match(r'^\d+$')

        # Проверяем, что "brand" является целым положительным числом
        is_positive_integer_brand = self.brand['brand'].str.match(r'^\d+$')

        # Комбинируем два условия и меняем местами записи
        mask = is_invalid_brand_id & is_positive_integer_brand
        self.brand.loc[mask, columns] = self.brand.loc[mask, columns].values[0][::-1]

        # Удаляем повторяющиеся значения поля "brand_id"
        self.brand.drop_duplicates(subset='brand_id', keep='first', inplace=True)


    def __transform_category_data(self):
        """
        Преобразует данные в таблице 'category'.
        """

        # Удаляем дубликаты по первичному ключу 'category_id'
        self.category.drop_duplicates(
            subset=['category_id'], keep='first', inplace=True)


    def __transform_product_data(self):
        """
        Преобразует данные в таблице 'product'.
        """

        # Заменяем значение 'О00' на 'O00' в столбце 'category_id'
        self.product['category_id'] = self.product['category_id'].replace('О00', 'O00')

        # Проверка наличия категории в списке 'category' и замена неимеющихся на 'NC'
        category = set(self.category['category_id'])
        mask = ~self.product['category_id'].isin(category)
        self.product.loc[mask, 'category_id'] = 'NC'

        # Удаляем дубликаты по первичному ключу 'product_id'
        self.product.drop_duplicates(
            subset=['product_id'], keep='first', inplace=True)

        # Проверяем, что "name_short" не является числом
        mask = self.product['name_short'].str.match(r'^\d+$')
        self.product = self.product.loc[~mask]


    def __transform_stock_data(self):
        """
        Преобразует данные в таблице 'stock'.
        """
        
        # Преобразуем столбец "available_quantity" в числовой тип
        self.stock['available_quantity'] = pd.to_numeric(
            self.stock['available_quantity'], errors='coerce')
        
        # Проверка на условия:
        # "product_id" - целое положительное число
        # "available_quantity" - положительное числа
        mask = (
            self.stock['product_id'].str.isnumeric() & 
            self.stock['available_quantity'].apply(
                lambda x: isinstance(x, (float, int)) and x > 0)
            )
        
        # Разделение данных на прошедшие проверку и непрошедшие
        self.stock = self.stock.loc[mask]

        # Убираем пустые значения в столбце 'cost_per_item'
        mask = self.stock['cost_per_item'].apply(lambda x: x != '')
        self.stock = self.stock.loc[mask]

        # Убираем значения из stock, которых нет в product, по product_id
        product = set(self.product['product_id'])
        mask = self.stock['product_id'].isin(product)
        self.stock = self.stock.loc[mask]

        # Убираем значения из stock, которых нет в shop, по pos
        shop = set(self.shop['pos'])
        mask = self.stock['pos'].isin(shop)
        self.stock = self.stock.loc[mask]

        # Конвертация даты из строкового формата в числовой формат
        self.stock['available_on'] = pd.to_numeric(
            self.stock['available_on'], errors='coerce')

        # Конвертация даты из числового формата в обычный формат даты
        self.stock['available_on'] = pd.to_datetime(
            self.stock['available_on'], origin='1899-12-30', unit='D')

        # Удаляем дубликаты по первичному ключу ('available_on', 'product_id', 'pos')
        self.stock.drop_duplicates(
            subset=['available_on', 'product_id', 'pos'], keep='first', inplace=True)


    def __transform_transaction_data(self):
        """
        Преобразует данные в таблице 'transaction'.
        """
        
        # Проверка на условия:
        # "quantity" - не пустое
        # "price" - не пустое
        mask = (
            self.transaction['quantity'].apply(lambda x: x != '') &
            self.transaction['price'].apply(lambda x: x != ''))
        
        # Убираем пустые значения
        self.transaction = self.transaction[mask]

        # Убираем значения из transaction, которых нет в product, по product_id
        product = set(self.product['product_id'])
        mask = self.transaction['product_id'].isin(product)
        self.transaction = self.transaction.loc[mask]

        # Убираем значения из transaction, которых нет в tran_shop, по transaction_id
        tran_shop = set(self.tran_shop['transaction_id'])
        mask = self.transaction['transaction_id'].isin(tran_shop)
        self.transaction = self.transaction.loc[mask]

        # Удаляем дубликаты по первичному ключу 'transaction_id'
        self.transaction.drop_duplicates(
            subset=['transaction_id', 'product_id'], keep='first', inplace=True)
