from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs

from load_db_data import ETLProcessor

def connection_uri_to_dict(connection_uri):
    url = urlparse(connection_uri)
    return {
        'host': url.hostname,
        'database': url.path.lstrip('/'),
        'user': url.username,
        'password': url.password,
        'port': url.port,
    }


# Определяем аргументы для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Функция, которая будет вызывать загрузку данных
def load_data_from_db(**kwargs):
    # Получаем параметры подключения из Connection
    sources_db_conn_params = BaseHook.get_connection('source').get_uri()
    writer_db_conn_params = BaseHook.get_connection('write').get_uri()
    sources_db_conn_params = connection_uri_to_dict(sources_db_conn_params)
    writer_db_conn_params = connection_uri_to_dict(writer_db_conn_params)

    # Создаем экземпляр класса ETLProcessor
    etl = ETLProcessor(sources_db_conn_params, writer_db_conn_params)
    etl.transform_raw_data()
    etl.load_correct_data()


# Создаем DAG
with DAG('load_data_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    # Оператор для загрузки данных
    load_data_task = PythonOperator(
        task_id='load_data_task',
        python_callable=load_data_from_db,
        provide_context=True,
    )


# Определяем порядок выполнения задач
load_data_task
