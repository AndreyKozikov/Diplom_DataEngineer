from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from links_parser import *
from coupon_parser import *
from description_parser import *
from coupon_transform import *
from bonds_transform import *
from airflow.models import Variable
import json
from coupon_normalise import *
from description_normalise import *

from setuptools.command.setopt import config_file


def config_options_load(**kwargs):
    config_file_path = Variable.get("config_dir", default_var="/opt/airflow/dags/config.json")
    print(config_file_path)
    with open(config_file_path, "r") as config_file:
        json_config = json.load(config_file)
    print(json_config)
    ti = kwargs['ti']  # Извлечение TaskInstance
    ti.xcom_push(key='config', value=json_config)

default_args = {
    'owner': 'airflow',
    'retries': 0,  # количество попыток перезапуска задачи при ошибке
    'retry_delay': timedelta(minutes=2),  # задержка между перезапусками
    'start_date': days_ago(1),  # дата, с которой DAG будет запускаться
    'catchup': False,  # если False, не выполнять DAG для всех дат в прошлом
}

dag = DAG(
    'Bonds_parser',  # Имя DAG
    default_args=default_args,
    description='DAG парсинга облигаций с dohod.ru',
    schedule_interval=None,  # можно задать cron-выражение для расписания
)

config_file_load = PythonOperator (
    task_id='config_file_load',
    python_callable=config_options_load,
    provide_context=True,  # позволяет передавать контекст в функцию
    dag=dag,
)

links_parser = PythonOperator(
    task_id='link_parser',  # ID задачи
    python_callable=links_parser_task,  # Функция, которая будет выполнена
    provide_context=True,  # позволяет передавать контекст в функцию
    dag=dag,  # Привязка к DAG
)

coupon_parser = PythonOperator(
    task_id='coupon_parser_task',  # ID задачи
    python_callable=coupon_parser_task,  # Функция, которая будет выполнена
    provide_context=True,  # позволяет передавать контекст в функцию
    dag=dag,  # Привязка к DAG
)

description_parser = PythonOperator(
    task_id='description_parser_task',  # ID задачи
    python_callable=description_parser_task,  # Функция, которая будет выполнена
    provide_context=True,  # позволяет передавать контекст в функцию
    dag=dag,  # Привязка к DAG
)

coupon_transform = PythonOperator(
    task_id='coupon_transform_task',  # ID задачи
    python_callable=coupon_transform_task,  # Функция, которая будет выполнена
    provide_context=True,  # позволяет передавать контекст в функцию
    dag=dag,  # Привязка к DAG
)

bonds_transform = PythonOperator(
    task_id='bonds_transform_task',  # ID задачи
    python_callable=bonds_transform_task,  # Функция, которая будет выполнена
    provide_context=True,  # позволяет передавать контекст в функцию
    dag=dag,  # Привязка к DAG
)

coupon_normalise = PythonOperator(
    task_id='coupon_normalise_task',  # ID задачи
    python_callable=coupon_normalise_task,  # Функция, которая будет выполнена
    provide_context=True,  # позволяет передавать контекст в функцию
    dag=dag,  # Привязка к DAG
)

description_normalise = PythonOperator(
    task_id='description_normalise_task',  # ID задачи
    python_callable=description_normalise_task,  # Функция, которая будет выполнена
    provide_context=True,  # позволяет передавать контекст в функцию
    dag=dag,  # Привязка к DAG
)

config_file_load >> links_parser >> [coupon_parser, description_parser]
coupon_parser >> coupon_transform >> coupon_normalise
description_parser >> bonds_transform >> description_normalise
