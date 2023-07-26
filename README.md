# Airflow-project

Проект Airflow-project представляет собой систему автоматизации и планирования задач (workflow) с использованием Apache Airflow. В нем представлен пример использования технологии Airflow для загрузки информации из исходной базы данных, их преобразования и записи в целевую.

## Структура проекта
<pre>
<code>
Airflow-project/
├─ docker/
│  └─ docker-compose.yaml
├─ ddl/
│  ├─ dds_create.sql
│  ├─ dm_create.sql
│  ├─ temp_dds_create.sql
│  └─ temp_dm_create.sql
└─ airflow/
   ├─ plugins/
   ├─ logs/
   └─ dags/
      ├─ airflowignore
      ├─ dag_dds_to_dm.py
      ├─ dag_sources_to_dds.py
      ├─ dds_to_dm.py
      ├─ sources_to_dds.py
      └─ airflowignore_files/
         ├─ etl_process_controller.py
         ├─ conn_to_schem/
         │  ├─ to_dds.py
         │  ├─ to_dm.py
         │  └─ to_sources.py
         └─ transform_rules/
            ├─ dds_to_dm.py
            └─ sources_to_dds.py
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
- dags - здесь располагаются DAG-файлы (Directed Acyclic Graph), описывающие рабочие процессы и связи между задачами. Подробное рассмотрение данной дирректории будет проведено далее.

## Дирректория dags
### DAG-файлы

### Структура взаимодействия
Структура взаимодействия элементов в данной дирректории (без учета DAGs) изображена на рисунке ниже:
<p align="center">
  <img src="https://github.com/Eugene531/Airflow-project/assets/94804642/dabcf967-b933-4aca-8dbc-bcac0fa8ef98" alt="Sublime's custom image"/>
</p>

Описание компонентов рисунка:

1. Дирректория 'conn_to_schem'.
Это папка, в которой определены специальные классы для взаимодействия с каждой из схем. Каждый из этих классов представляет собой некий коннектор с конкретной схемой БД, через которого можно обращаться к ней для выгрузки (Export) и загрузки (Load) данных. Это реализуется через менеджер контекстов 'with', что гарантирует целостность данных (т.е. автоматическое применение rollback, если было какое-либо исключение). Здесь определены следующие классы:
- ConnectorToDds (файл - to_dds.py) - коннектор для схемы 'dds'.
- ConnectorToDm (файл - to_dm.py) - коннектор для схемы 'dm'.
- ConnectorToSources (файл - to_sources.py) - коннектор для схемы 'sources'.

2. Дирректория 'transform_rules'.

3. Файлы 'sources_to_dds.py' и 'dds_to_dm.py'

4. Файл 'etl_process_controller.py'

Для того, чтобы запустить процесс ETL между двумя БД, первым делом импортируются специальные классы, которые лежат в дирректории 'conn_to_schem'. В данном проекте это три класса: ConnectorToDds (файл: to_dds.py), ConnectorToDm (файл: to_dm.py) и ConnectorToSources (файл: to_sources.py). Они используются 

## Запуск проекта
1. Установите Docker и Docker Compose, если они еще не установлены.
2. Склонируйте репозиторий с проектом.
3. В папке проекта выполните команду docker-compose up для развертывания Airflow и связанных компонентов в контейнерах Docker.
4. Перейдите в веб-интерфейс Airflow, доступный по адресу http://localhost:8080, запланируйте и запустите DAG с названием "load_data_task" для выполнения задач.

## Зависимости
- Docker (v. 24.0.2) и Docker Compose для контейнеризации приложения и его компонентов.
- Apache Airflow (v. 2.6.3) для управления и планирования задач в рамках проекта.
- База данных PostgreSQL (v. 12.10) для хранения исходных и результатирующих данных.
- Python (v. 3.10).
