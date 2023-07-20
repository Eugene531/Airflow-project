# Airflow-project

Проект Airflow-project представляет собой систему автоматизации и планирования задач (workflow) с использованием Apache Airflow. В этом проекте представлен пример использования Airflow для загрузки данных из базы данных, их преобразования и записи в другую.

## Структура проекта
<pre>
<code>
Airflow-project/
├─ docker/
│  └─ docker-compose.yaml
├─ ddl/
│  └─ dds_create.sql
└─ airflow/
   ├─ plugins/
   ├─ logs/
   └─ dags/
      ├─ load_dag.py
      └─ load_db_data.py
</code>
</pre>

## Компоненты
### Docker
Папка docker содержит файл docker-compose.yaml, который определяет конфигурацию Docker Compose для развертывания Airflow и связанных компонентов в контейнерах Docker.
### DDL
Папка ddl содержит файл dds_create.sql, который содержит SQL-скрипт для создания схемы и таблиц в базе данных для проекта.
### Airflow
Папка airflow содержит все связанные файлы и папки, необходимые для настройки Airflow и выполнения задач, в частности:
- plugins - эта папка предназначена для хранения пользовательских плагинов для Apache Airflow;
- logs - в этой папке будут храниться логи выполнения задач;
- dags - здесь располагаются DAG-файлы (Directed Acyclic Graph), описывающие рабочие процессы и связи между задачами.

## DAG-файлы
### load_dag.py
Файл load_dag.py содержит определение DAG (Directed Acyclic Graph) для процесса загрузки данных из исходной базы данных в БД для записи. Параметры подключения к базам данных получаются из Connection. Затем создается экземпляр класса ETLProcessor, который выполняет преобразование и загрузку данных. 
### load_db_data.py
