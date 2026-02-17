# dags/weatherMap_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

import datetime
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor

# Plus besoin de sys.path.insert grâce à PYTHONPATH !
from collect_data import fetch_weather_data
from transform_data import transform_data_into_csv
from train_data import compute_model_score, train_and_save_model,prepare_data, train_best_model

def compute_lr_score_wrapper(task_instance):
    """Wrapper autonome : prépare les données ET calcule le score"""
    X, y = prepare_data()                          # Prépare les données
    score = compute_model_score(                    # Calcule le score
        LinearRegression(), X, y
    )
    print(f"📊 Score LinearRegression : {score:.4f}")
    task_instance.xcom_push(
        key="score_lr",
        value=score
    )

def compute_dt_score_wrapper(task_instance):
    """Wrapper autonome : prépare les données ET calcule le score"""
    X, y = prepare_data()                          # Prépare les données
    score = compute_model_score(                    # Calcule le score
        DecisionTreeRegressor(), X, y
    )
    print(f"📊 Score DecisionTree : {score:.4f}")
    task_instance.xcom_push(
        key="score_dt",
        value=score
    )

def compute_rf_score_wrapper(task_instance):
    """Wrapper autonome : prépare les données ET calcule le score"""
    X, y = prepare_data()                          # Prépare les données
    score = compute_model_score(                    # Calcule le score
        RandomForestRegressor(), X, y
    )
    print(f"📊 Score Random Forest : {score:.4f}")
    task_instance.xcom_push(
        key="score_rf",
        value=score
    )
    


with DAG(
    dag_id='weather_dag',
    description='Récupération des données météo',
    tags=['tutorial', 'datascientest', 'weather'],
    schedule_interval=datetime.timedelta(minutes=60),
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(1),
        'retries': 3,
        'retry_delay': datetime.timedelta(minutes=5),
    },
    catchup=False
) as my_dag:

    fetch_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )

    transform_task_all = PythonOperator(
        task_id='transform_data_into_csv_all',
        python_callable=transform_data_into_csv,
        op_kwargs={
            'filename': 'fulldata.csv'
        }
    )

    transform_task_20 = PythonOperator(
        task_id='transform_data_into_csv_20',
        python_callable=transform_data_into_csv,
        op_kwargs={
            'n_files': 20
        }
    )
        
    with TaskGroup("group_Train") as group_Train:
        task_Train1 = PythonOperator(
            task_id='score_lr_task',
            python_callable=compute_lr_score_wrapper  
        )
        task_Train2 = PythonOperator(
            task_id='score_dt_task',
            python_callable=compute_dt_score_wrapper 
        )
        task_Train3 = PythonOperator(
            task_id='score_rf_task',
            python_callable=compute_rf_score_wrapper 
        )
        
    train_task = PythonOperator(
        task_id='train_best_model',
        python_callable=train_best_model
    )


    fetch_task >> [transform_task_all, transform_task_20]
    transform_task_all >> group_Train
    group_Train >> train_task
    