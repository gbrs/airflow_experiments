from airflow.decorators import dag, task
from datetime import datetime, timedelta


@dag(
    dag_id='super_fast_logger',
    start_date=datetime(2024, 1, 1, 0, 0, 10),
    schedule_interval=timedelta(seconds=10),
    catchup=False,
    max_active_runs=1,
)
def super_fast_logger():
    @task
    def log_time():

        with open('/opt/airflow/data/output/time_log.txt', 'a') as file:
            file.write(f'{datetime.now().strftime("%H:%M:%S.%f")[:-3]}\n')

    # Просто вызываем задачу
    log_time()


# Создаем DAG
fast_dag = super_fast_logger()

