import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator

path = os.path.expanduser('~/airflow_hw')
#path = os.path.expanduser('C:/Users/Админ/airflow_hw')
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from modules.pipeline import pipeline
# <YOUR_IMPORTS>
from modules.predict import predict

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 7, 15),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
    'execution_timeout': dt.timedelta(minutes=30),
}

with DAG(
        dag_id='car_price_prediction',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
        dag=dag,
    )
    # <YOUR_CODE>
    predict = PythonOperator(
        task_id='predict',
        python_callable=predict,
        dag=dag,
    )

    pipeline >> predict
