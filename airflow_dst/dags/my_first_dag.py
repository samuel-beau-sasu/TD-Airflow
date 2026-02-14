from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime

with DAG(
    dag_id='my_very_first_dag',
    description='My first DAG created with DataScientest',
    tags=['tutorial', 'datascientest'],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(2),
    }
) as my_dag:

    # Définition de la fonction à exécuter
    def print_date_and_hello():
        print(datetime.datetime.now())
        print('Hello from Airflow')

    def print_date_and_hello_again():
        print('Hello from Airflow again')

    my_task = PythonOperator(
        task_id='my_very_first_task',
        python_callable=print_date_and_hello,
    )

    my_task2 = PythonOperator(
        task_id='my_second_task',
        python_callable=print_date_and_hello_again,
    )

my_task >> my_task2