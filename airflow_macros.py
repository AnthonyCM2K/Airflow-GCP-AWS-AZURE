from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import date
from random import random

default_args = {
    'owner': 'Anthony Criollo',
    'start_date': days_ago(1)
}

dag_arg = {
    'dag_id': 'airflow_macros',
    'schedule_interval': '@daily',
    'catchup': False,
    'default_arg': default_args,
    'user_defined_macros': {
        'mensaje': 'Aqui DAG CON MACRO en Apache Airflow!',
        'fecha': str(date.today())
    }
}


with DAG(**dag_args) as dag:
    #
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "---> {{ mensaje }}, Fecha: $TODAY y TIMESTAMP ES: {{ ts_nodash_with_tz }}"',
        env={ 'TODAY': '{{ fecha }}' }
    )

    def print_random_number(number=None, otro=None):
        for i in range(number):
            print(f'ESTE ES EL RAMDOM NUMBER {i+1}', random())

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=print_random_number,
        op_kwargs={ 'number': 10 }
    )

# DEPENDENCIAS
bash_task >> python_task