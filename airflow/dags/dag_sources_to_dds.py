import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta

# Определяем аргументы для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создаем DAG с именем 'run_etl_sources_to_dds'
with DAG(
    'run_etl_sources_to_dds',
    default_args=default_args,
    schedule_interval='0 11 1 * *',  # Каждый месяц в 11:00, первого числа
    catchup=False # Запрещаем выполнять старые пропущенные задачи
    ) as dag:

    # Получаем параметры подключения из Connection
    read_db_conn_params = BaseHook.get_connection('source').get_uri()
    read_uri = read_db_conn_params.split('?', 1)[0]
    read_uri = read_uri.replace("postgres", "postgresql", 1)

    write_db_conn_params = BaseHook.get_connection('write').get_uri()
    write_uri = write_db_conn_params.split('?', 1)[0]
    write_uri = write_uri.replace("postgres", "postgresql", 1)

    module = 'sources_to_dds'

    # Получаем путь к директории, где находится текущий DAG-файл
    dir = os.path.dirname(__file__)

    # Оператор для вызова скрипта read_to_dds.py с помощью BashOperator
    run_etl_sources_to_dds = BashOperator(
        task_id='run_etl_sources_to_dds',
        bash_command = f'python {dir}/main_func.py "{read_uri}" "{write_uri}" "{module}"'
    )

# Определяем порядок выполнения задач
run_etl_sources_to_dds
