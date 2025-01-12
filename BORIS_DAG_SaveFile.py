import random
import math
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from google.colab import files
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': True,
    'email_on_success': True,
    'email': ['vichugova.anna@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ANNA_DAG_SaveFile',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

def write_to_file(**context):
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    result = context['ti'].xcom_pull(task_ids='task4')
    filename = f'/content/airflow/results/{execution_date}.txt'
    with open(filename, 'a') as f:
        f.write(result + '\n')

        
# Ниже определяются операторы, которые будут выполняться в DAG
# Формирование текста с номером запуска DAG в task1, вывод текущей даты в task2,
# вывод текущего времени в task3, вывод результатов выполнения task2 и task3 в task4,
# и запись результатов в txt-файл с названием текущей даты в task5

task1 = BashOperator(
    task_id='task1',
    bash_command='echo "Запуск DAG {{ dag_run.run_id }}" | tee /tmp/dag_run_id.txt',
    dag=dag,
)

task2 = BashOperator(
    task_id='task2',
    bash_command='echo "Сегодня $(date +%d-%m-%Y)"',
    dag=dag,
)

task3 = BashOperator(
    task_id='task3',
    bash_command='echo "В $(date +%H:%M:%S)"',
    dag=dag,
)

task4 = BashOperator(
    task_id='task4',
    bash_command='echo "{{ ti.xcom_pull(task_ids=\'task1\') }}. {{ ti.xcom_pull(task_ids=\'task2\') }}. {{ ti.xcom_pull(task_ids=\'task3\') }}"',
    dag=dag,
)

task5 = PythonOperator(
    task_id='task5',
    python_callable=write_to_file,
    dag=dag
)

task1 >> [task2, task3] >> task4 >> task5