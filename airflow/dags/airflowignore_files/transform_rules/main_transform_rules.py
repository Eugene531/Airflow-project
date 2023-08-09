from importlib import import_module


class TransformRules:
    """
    Нужен для динамической загрузки модуля по его имени "module_name" и вызова 
    метода get_transformed_data из него для получения трансформированных данных.

    Входные данные:
        module_name: str
            Имя модуля, который будет импортироваться.

    Методы:
        get_transformed_data(raw_data) -> dict
            Возвращает словарь, где ключ - имя таблицы, а значение - pandas.DataFrame.
    """

    def __init__(self, module_name: str) -> None:
        self.__class_module = import_module(module_name) # Импортируем указанный модуль
        
        
    def get_transformed_data(self, raw_data: dict) -> dict:
        """
        Метод для получения трансформированных данных, которые получаюся в соответствии
        с правилами из указанного модуля.

        Входные данные:
            raw_data: dict
                Словарь из сырых данных, где ключ - имя таблицы, а значение - pandas.DataFrame
        """

        # Вызываем метод get_transformed_data из загруженного модуля TransformData
        return self.__class_module.TransformData().get_transformed_data(raw_data)
