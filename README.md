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
Файл load_dag.py содержит определение DAG (Directed Acyclic Graph) для процесса загрузки данных из исходной базы данных в БД для записи. Параметры подключения к базам данных получаются из Connection. Затем создается экземпляр класса ETLProcessor, который выполняет преобразование и загрузку данных. 
### load_db_data.py
load_db_data.py - это файл, содержащий класс ETLProcessor, который выполняет преобразование данных, необходимое для загрузки в целевую БД.
##
<p align="center">
  <img src="https://github.com/Eugene531/Airflow-project/assets/94804642/b6bd1e7a-e3e1-40bd-aea7-f14717e3f309" alt="Sublime's custom image"/>
</p>

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
