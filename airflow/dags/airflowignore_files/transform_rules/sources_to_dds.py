import pandas as pd


class TransformData:
    """
    Используется для реализации процесса транфсормации (Transform) данных в 
    процессе их передачи из схемы 'sources' в схему 'dss'

    Методы:
        get_transformed_data(raw_data: dict) -> dict
            Метод для трансформации данных (raw_data) для очистки данных схемы 'sources'.
            Возвращает словарь, где ключ - имя таблицы, а значение - pandas.DataFrame
    """

    def __init__(self) -> None:
        # Таблицы, которые будут использоваться для трансформации данных
        self.__tables_for_transform: list = [
            'shop',
            'tran_shop',
            'brand',               # Таблица с данными о брендах
            'category',            # Таблица с данными о категориях
            'product',             # Таблица с данными о продуктах
            'stock',               # Таблица с данными о складах
            'transaction',         # Таблица с данными о транзакциях
        ]

        # Таблицы с ошибками
        self.__error_tables: list = [
            'product_errors',      # Таблица с ошибками в данных о продуктах
            'stock_errors',        # Таблица с ошибками в данных о складах
            'transaction_errors',  # Таблица с ошибками в данных о транзакциях
        ]

        # Значения по умолчанию таблиц для трансформации
        self.__brand: pd.DataFrame = pd.DataFrame()
        self.__category: pd.DataFrame = pd.DataFrame()
        self.__product: pd.DataFrame = pd.DataFrame()
        self.__stock: pd.DataFrame = pd.DataFrame()
        self.__transaction: pd.DataFrame = pd.DataFrame()

        # Значения по умолчанию таблиц с ошибками для перезаписи
        self.__product_errors: pd.DataFrame = pd.DataFrame()
        self.__stock_errors: pd.DataFrame = pd.DataFrame()
        self.__transaction_errors: pd.DataFrame = pd.DataFrame()
    

    def get_transformed_data(self, raw_data: dict) -> dict:
        """
        Метод для получения данных из таблиц для схемы 'dds'.

        Returns:
        Словарь, где ключами служат имена таблиц, а значениями - DataFrame с 
        данными соответствующих таблиц.
        """
        
        # Определяем данные таблиц для перезаписи (при помощи setattr)
        self.__set_overwrite_tables_data(raw_data)

        # Запуск методов для преобразования данных (для каждой из таблиц свой метод)
        self.__transform_brand_data()
        self.__transform_category_data()
        self.__transform_product_data()
        self.__transform_stock_data()
        self.__transform_transaction_data()

        # Возвращаем приватные атрибуты - значения таблиц для перезаписи
        private = lambda n: f'_{self.__class__.__name__}__{n}'
        
        # Получаем словарь названий таблиц для перезаписи и их значений
        data = {tab: getattr(self, private(tab)) for tab in self.__tables_for_transform}
        
        # Добавляем в словарь данные о таблицах с ошибками
        data.update({tab: getattr(self, private(tab)) for tab in self.__error_tables})

        return data
    

    def __set_overwrite_tables_data(self, raw_data: dict) -> None:
        """
        Метод для определения данных в таблицах для перезаписи.
        Информация берется из исходной БД (raw_data).

        Исключения:
        KeyError:
            названия таблиц для перезаписи не найдены в исходной БД (raw_data).
        """

        # Проверяем, что данные из исходной БД (raw_data) не равны None.
        # Если данных нет, то ничего не делаем, т.к. нет информации для перезаписи.
        if raw_data is not None:

            for table_name in self.__tables_for_transform:

                # Пытаемся получить данные для текущей таблицы (table_name) из исходной БД.
                try:
                    data = raw_data[table_name]
                except KeyError:
                    # Вывод предупреждения и продолжение выполнения программы без некоторых данных
                    print(f"Предупреждение: Данные для таблицы '{table_name}' не найдены.")
                    continue

                # Если данные для текущей таблицы были успешно получены из источника,
                # то устанавливаем их в соответствующий атрибут экземпляра класса.
                private = lambda n: f'_{self.__class__.__name__}__{n}'
                setattr(self, private(table_name), data)

    
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

        if len(self.__brand.loc[mask, columns]) > 0:
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
        
        # Конвертация даты из строкового формата в числовой формат
        self.__stock_errors['available_on'] = pd.to_numeric(
            self.__stock_errors['available_on'], errors='coerce'
            )

        # Конвертация даты из числового формата в обычный формат даты
        self.__stock_errors['available_on'] = pd.to_datetime(
            self.__stock_errors['available_on'], origin='1899-12-30', unit='D'
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
