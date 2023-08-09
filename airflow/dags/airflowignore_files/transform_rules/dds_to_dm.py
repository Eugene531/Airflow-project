import pandas as pd
from datetime import datetime


class TransformData:
    """
    Используется для реализации процесса транфсормации (Transform) данных в 
    процессе их передачи из схемы 'dds' в схему 'dm'

    Методы:
        get_transformed_data(raw_data: dict) -> dict
            Метод для трансформации данных (raw_data) для нахождения метрик схемы 'dm'.
            Возвращает словарь, где ключ - имя таблицы, а значение - pandas.DataFrame
    """

    def __init__(self) -> None:
        self.__tables_to_ret = [
            'transactions_group_ymd',
            'average_check',
            'purchases',
            'mean_monthly_product_stats',
            'total_stats',
        ]


    def get_transformed_data(self, raw_data: dict) -> dict:
        """
        Метод для трансформации данных (raw_data) для нахождения метрик схемы 'dm'.
        
        Входные параметры:
        raw_data: dict
            Словарь из сырых данных. В нем ключ - имя таблицы, значение - pandas.DataFrame

        Returns:
        Возвращает словарь, где ключ - имя таблицы, а значение - pandas.DataFrame
        """

        product = raw_data['product']
        category = raw_data['category']
        shop = raw_data['shop']
        tran_shop = raw_data['tran_shop']
        transaction = raw_data['transaction']
        brand = raw_data['brand']
        stock = raw_data['stock']

        transactions_group_ymd = self.__transactions_group_ymd(
            transaction, tran_shop, product, brand, category)
        
        average_check = self.__average_check(transaction, tran_shop)

        purchases = self.__purchases(transaction, stock, product, category)

        mean_monthly_product_stats = self.__mean_monthly_product_stats(
            transaction, product)
        
        total_stats = self.__total_stats(tran_shop, transaction)

        datas = [transactions_group_ymd, average_check, purchases, 
                 mean_monthly_product_stats, total_stats]

        return dict(zip(self.__tables_to_ret, datas))

    
    def __transactions_group_ymd(self, 
                                 transaction, tran_shop, product, brand, category):
        """
        Метод для формирования таблицы 'transactions_group_ymd'
        """
        # Присоединение таблиц с использованием операций left join
        transaction_data = transaction.copy()

        merged_data = (
            transaction_data
            .merge(tran_shop, on='transaction_id', how='left')
            .merge(product, on='product_id', how='left')
            .merge(brand, on='brand_id', how='left')
            .merge(category, on='category_id', how='left')
        )

        # Добавление столбца 'day' с помощью функции EXTRACT
        merged_data['day'] = merged_data['recorded_on'].dt.dayofweek
        
        # Преобразование столбца 'recorded_on' в формат 'YYYY-MM'
        merged_data['date'] = merged_data['recorded_on'].dt.strftime('%Y-%m')

        merged_data.rename(columns={'price': 'sum_price'}, inplace=True)

        # Выполнение агрегации данных
        aggregated_data = (
            merged_data
            .groupby(['date', 'day', 'pos', 'category_name', 'name_short', 'sum_price'])
            .agg(sum_quantity=('quantity', 'sum'))
            .reset_index()
        )

        # Сортировка результатов
        sorted_data = (
            aggregated_data
            .sort_values(by=[
                'date', 'pos', 'category_name', 'name_short', 'sum_price', 'sum_quantity'])
        )

        # Добавление столбца "UPDATE_DATE" с текущей датой и временем
        sorted_data['UPDATE_DATE'] = datetime.now()

        return sorted_data
    

    def __average_check(self, transaction, tran_shop):
        """
        Метод для формирования таблицы 'average_check'
        """

        transaction_data = transaction.copy()
        
        # Преобразование столбца "recorded_on" в формат 'YYYY-MM'
        transaction_data['date'] = pd.to_datetime(
            transaction_data['recorded_on']).dt.strftime('%Y-%m')

        # Создание вспомогательного DataFrame subq
        subq = transaction_data.merge(tran_shop, on='transaction_id', how='left')
        subq['total'] = subq.groupby('transaction_id')['price'].transform('sum')

        # Группировка и вычисление среднего значения total для каждой даты и pos
        result_data = subq.groupby(['date', 'pos'])['total'].mean().reset_index()

        # Добавление столбца "UPDATE_DATE" с текущей датой и временем
        result_data['UPDATE_DATE'] = datetime.now()

        return result_data


    def __purchases(self, transaction, stock, product, category):
        """
        Метод для формирования таблицы 'purchases'
        """

        stock_data = stock.copy()

        # Группируем данные по 'product_id' и 'pos', выбираем максимальную 
        # дату ('available_on') и суммируем 'available_quantity',
        # также используем 'cost_per_item' в качестве агрегирующей функции 
        # (он не меняется при агрегации)
        result = stock_data.groupby(['product_id']).agg({
            'available_on': 'max',
            'available_quantity': 'sum',
            'cost_per_item': 'first'
        }).reset_index()


        # Объединяем таблицы product и category по столбцу 'category_id' 
        # с помощью left join
        result = (
            result
            .merge(product[['product_id', 'name_short', 'category_id']], 
                   on='product_id', how='left')
            .merge(category[['category_id', 'category_name']], 
                   on='category_id', how='left')
        )

        # Удаляем поле 'category_id' из результата
        result.drop(columns=['category_id'], inplace=True)

        # Обработка таблицы transactions
        transaction_data = transaction.copy()

        transaction_data['recorded_on'] = pd.to_datetime(
            transaction_data['recorded_on']).dt.strftime('%Y-%m')

        # Отбираем только те строки, где 'order_type_id' = 'BUY'
        filtered_transactions = transaction_data[
            transaction_data['order_type_id'] == 'BUY']

        # Группируем данные по 'product_id' и 'month', суммируем 'quantity'
        grouped_transactions = filtered_transactions.groupby(
            ['product_id', 'recorded_on']).agg({
            'quantity': 'sum'
        }).reset_index()

        result_year_month = pd.to_datetime(
            result['available_on']).dt.strftime('%Y-%m')[0]

        grouped_transactions = grouped_transactions[
            grouped_transactions.recorded_on == result_year_month]
        
        # Объединяем result с grouped_transactions по столбцу 'product_id' с
        #  помощью left join
        result = result.merge(grouped_transactions, on='product_id', how='left')

        result.drop(columns=['recorded_on'], inplace=True)
        
        # Добавление столбца "UPDATE_DATE" с текущей датой и временем
        result['UPDATE_DATE'] = datetime.now()

        # Замена значений NaN на 0
        result['quantity'] = result['quantity'].fillna(0)

        # Переименовываем поле 'available_quantity' в 'stock_total'
        result.rename(columns={'available_quantity': 'stock_total'}, inplace=True)

        # Переименовываем поле 'available_on' в 'update_date_in_stock'
        result.rename(columns={'available_on': 'update_date_in_stock'}, inplace=True)
        
        # Переименовываем поле 'quantity' в 'sold_total'
        result.rename(columns={'quantity': 'sold_total'}, inplace=True)

        # Переименовываем поле 'category_name' в 'category'
        result.rename(columns={'category_name': 'category'}, inplace=True)

        return result

        
    def __mean_monthly_product_stats(self, transaction, product):
        """
        Метод для формирования таблицы 'mean_monthly_product_stats'
        """

        transaction_data = transaction.copy()
        
        transaction_data['date'] = transaction_data['recorded_on'].dt.strftime('%Y-%m')
        
        # Отбираем только те строки, где 'order_type_id' = 'BUY'
        filtered_transactions = transaction_data[
            transaction_data['order_type_id'] == 'BUY']

        # Группируем данные по 'product_id' и 'date' и вычисляем среднее значение 
        # кол-ва проданных продуктов и цену
        average_quantity_per_month = filtered_transactions.groupby(
            ['product_id', 'date']).agg({'quantity': 'sum', 'price': 'mean'}).reset_index()

        # Создаем все комбинации 'product_id' и 'date'
        all_product_month_combinations = pd.MultiIndex.from_product([
            product['product_id'].unique(), transaction_data['date'].unique()], 
            names=['product_id', 'date'])
        
        # Создаем DataFrame с комбинациями и объединяем его с результатом агрегации 
        # по 'product_id' и 'month' с помощью right join
        merged_data = all_product_month_combinations.to_frame(index=False).merge(
            average_quantity_per_month, on=['product_id', 'date'], how='left')
        
        # Переименовываем поле 'price' в 'avg_price'
        merged_data.rename(columns={'price': 'avg_price'}, inplace=True)

        # Заполняем NaN значения в столбце 'quantity' нулями
        merged_data['quantity'] = merged_data['quantity'].fillna(0)

        # Заполняем NaN значения в столбце 'price' нулями
        merged_data['avg_price'] = merged_data['avg_price'].fillna(0)

        # Добавление столбца "UPDATE_DATE" с текущей датой и временем
        merged_data['UPDATE_DATE'] = datetime.now()

        return merged_data

        
    def __total_stats(self, tran_shop, transaction):
        """
        Метод для формирования таблицы 'total_stats'
        """

        transaction_data = transaction.copy()

        transaction_data['date'] = transaction_data['recorded_on'].dt.strftime('%Y-%m')
        
        filtered_transactions = transaction_data[
            transaction_data['order_type_id'] == 'BUY']
        
        filtered_transactions = filtered_transactions.merge(tran_shop, on='transaction_id', how='left')

        # Создаем новый столбец 'total_gross', в котором хранится 'quantity' * 'price'
        filtered_transactions['total_gross'] = filtered_transactions['quantity']\
              * filtered_transactions['price']

        average_quantity_per_month = filtered_transactions.groupby(['date', 'pos']).agg({
            'total_gross': 'sum',
            'quantity': 'sum',
            'transaction_id': 'count'
        }).reset_index()

        # Вычисляем средний чек ('avg_check')
        average_quantity_per_month['avg_check'] =\
            round(average_quantity_per_month['total_gross'] / average_quantity_per_month['transaction_id'], 0)
        
        average_quantity_per_month.drop(columns=['transaction_id'], inplace=True)

        # Переименовываем поле 'quantity' в 'total_volume'
        average_quantity_per_month.rename(
            columns={'quantity': 'total_volume'}, inplace=True)

        transaction_data['recorded_on'] = transaction_data['recorded_on'].dt.strftime('%Y-%m-%d')

        # Добавление столбца "UPDATE_DATE" с текущей датой и временем
        average_quantity_per_month['UPDATE_DATE'] = datetime.now()

        return average_quantity_per_month
    