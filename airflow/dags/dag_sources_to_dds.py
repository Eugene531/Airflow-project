import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta

# Определяем аргументы для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создаем DAG с именем 'etl_data_sources_to_dds'
with DAG(
    'etl_data_sources_to_dds',
    default_args=default_args,
    schedule_interval=None, # Нет расписания, DAG будет запускаться вручную
    catchup=False # Запрещаем выполнять старые пропущенные задачи
    ) as dag:

    # Получаем параметры подключения из Connection
    sources_db_conn_params = BaseHook.get_connection('source').get_uri()
    sources_uri, _ = sources_db_conn_params.split('?', 1)
    sources_uri = sources_uri.replace("postgres", "postgresql", 1)

    writer_db_conn_params = BaseHook.get_connection('write').get_uri()
    writer_uri, _ = writer_db_conn_params.split('?', 1)
    writer_uri = writer_uri.replace("postgres", "postgresql", 1)

    # Получаем путь к директории, где находится текущий DAG-файл
    dir = os.path.dirname(__file__)

    # Оператор для вызова скрипта sources_to_dds.py с помощью BashOperator
    run_etl_sources_to_dds = BashOperator(
        task_id='run_etl_sources_to_dds',
        bash_command = f'python {dir}/sources_to_dds.py "{sources_uri}" "{writer_uri}"'
    )

    trigger_dag2 = TriggerDagRunOperator(
        task_id='trigger_dag2',
        trigger_dag_id='etl_data_dds_to_dm'
    )

# Определяем порядок выполнения задач
run_etl_sources_to_dds >> trigger_dag2
