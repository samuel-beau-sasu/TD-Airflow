import random
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime


def successful_task():
    print('success')


def failed_task():
    raise Exception('This task did not work!')


def random_fail_task():
    random.seed()
    a = random.random() 
    print(a)
    if a < .9:
        raise Exception('This task randomly failed')

with DAG(
    dag_id='fork3_dag',
    tags=['tutorial', 'datascientest'],
    schedule_interval=datetime.timedelta(seconds=10),
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1)
    },
    catchup=False
) as my_dag:

    task1 = PythonOperator(
    task_id="my_failed_task",
    python_callable=failed_task,
    retries=5,
    retry_delay=datetime.timedelta(seconds=30),
    dag=my_dag,
    email_on_retry=True,
    email=['f_samuel.beau@gmail.com']
    )



