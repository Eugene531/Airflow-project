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

# Создаем DAG с именем 'run_etl_dds_to_dm'
with DAG(
    'run_etl_dds_to_dm',
    default_args=default_args,
    schedule_interval='0 12 1 * *',  # Каждый месяц в 11:00, первого числа
    catchup=False # Запрещаем выполнять старые пропущенные задачи
    ) as dag:

    # Получаем параметры подключения из Connection
    writer_db_conn_params = BaseHook.get_connection('write').get_uri()
    writer_uri = writer_db_conn_params.split('?', 1)[0]
    writer_uri = writer_uri.replace("postgres", "postgresql", 1)

    read_uri = writer_uri

    module = 'dds_to_dm'

    # Получаем путь к директории, где находится текущий DAG-файл
    dir = os.path.dirname(__file__)

    # Оператор для вызова скрипта sources_to_dds.py с помощью BashOperator
    run_etl_dds_to_dm = BashOperator(
        task_id='run_etl_dds_to_dm',
        bash_command = f'python {dir}/main_func.py "{read_uri}" "{writer_uri}" "{module}"'
    )

# Определяем порядок выполнения задач
run_etl_dds_to_dm