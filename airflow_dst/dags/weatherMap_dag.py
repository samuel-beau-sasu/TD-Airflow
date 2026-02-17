# dags/weatherMap_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

import datetime
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor

import pandas as pd
from sklearn.model_selection import cross_val_score
from joblib import dump
import os

import requests
import json


def fetch_weather_data(cities=None):
    if cities is None:
        try:
            from airflow.models import Variable
            cities = Variable.get("cities", deserialize_json=True)
        except Exception:
            cities = ['paris', 'london', 'washington']

    api_key = "06768d8496a01224cd8ddb348f312338"
    all_data = []

    for city in cities:
        url = (
            f"https://api.openweathermap.org/data/2.5/weather"
            f"?q={city}"
            f"&appid={api_key}"
        )
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            city_data = response.json()
            all_data.append(city_data)
            print(f"✅ {city} : {city_data['weather'][0]['description']}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur pour {city} : {e}")

    now = datetime.datetime.now()
    filename = now.strftime("%Y-%m-%d %H:%M") + ".json"

    # ✅ Chemin absolu dans le conteneur Docker
    #output_dir = "/opt/airflow/raw_files"
    output_dir = os.getenv(
        "AIRFLOW_RAW_FILES_PATH",  # Variable d'environnement
        "/app/raw_files"   # Valeur par défaut dans Docker
    )
    
    os.makedirs(output_dir, exist_ok=True)

    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w") as f:
        json.dump(all_data, f, indent=4)

    print(f"💾 Sauvegardé : {filepath}")
    return filepath

def transform_data_into_csv(n_files=None, filename='data.csv'):
    #parent_folder = "/opt/airflow/raw_files"
    parent_folder = os.getenv(
        "AIRFLOW_RAW_FILES_PATH",  # Variable d'environnement
        "/app/raw_files"   # Valeur par défaut dans Docker
    )
    
    files = sorted(os.listdir(parent_folder), reverse=True)
    if n_files:
        files = files[:n_files]

    dfs = []

    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data_temp = json.load(file)
        for data_city in data_temp:
            dfs.append(
                {
                    'temperature': data_city['main']['temp'],
                    'city': data_city['name'],
                    'pression': data_city['main']['pressure'],
                    'date': f.split('.')[0]
                }
            )

    df = pd.DataFrame(dfs)

    print('\n', df.head(10))
    
    #result_folder = '/opt/airflow/clean_data'
    result_folder = os.getenv(
        "AIRFLOW_RESULT_FILES_PATH",  # Variable d'environnement
        "/app/clean_data"   # Valeur par défaut dans Docker
    )

    df.to_csv(os.path.join(result_folder, filename), index=False)
    
def compute_model_score(model, X, y):
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()

    return model_score

def train_and_save_model(model, X, y, path_to_model=None):
    
    if path_to_model is None:
        path_to_model = os.getenv(
            "AIRFLOW_MODEL_PATH",  # Variable d'environnement
            "/app/clean_data/model.pckl"   # Valeur par défaut dans Docker
        )

    # training the model
    model.fit(X, y)
    # saving model
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)

def prepare_data(path_to_data=None):
    if path_to_data is None:
        path_to_data = os.getenv(
            "AIRFLOW_DATA_PATH",  # Variable d'environnement
            "/app/clean_data/fulldata.csv"   # Valeur par défaut dans Docker
        )
    # reading data
    df = pd.read_csv(path_to_data)
    # ordering data according to city and date
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []

    for c in df['city'].unique():
        df_temp = df[df['city'] == c]

        # creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

        # creating features
        for i in range(1, 10):
            df_temp.loc[:, 'temp_m-{}'.format(i)
                        ] = df_temp['temperature'].shift(-i)

        # deleting null values
        df_temp = df_temp.dropna()

        dfs.append(df_temp)

    # concatenating datasets
    df_final = pd.concat(
        dfs,
        axis=0,
        ignore_index=False
    )

    # deleting date variable
    df_final = df_final.drop(['date'], axis=1)

    # creating dummies for city variable
    df_final = pd.get_dummies(df_final)

    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    return features, target

def select_and_train_best_model(score_lr, score_dt, score_rf):
    """
    Fonction PURE : pas de dépendance Airflow !
    Compare les scores et sauvegarde le meilleur modèle.
    Testable en dehors d'Airflow.
    """
    models = [
        {
            'name': 'LinearRegression',
            'score': score_lr,
            'model': LinearRegression()
        },
        {
            'name': 'DecisionTreeRegressor',
            'score': score_dt,
            'model': DecisionTreeRegressor()
        },
        {
            'name': 'RandomForestRegressor',
            'score': score_rf,
            'model': RandomForestRegressor()
        },
    ]

    # Afficher tous les scores
    for m in models:
        print(f"📊 {m['name']} : {m['score']:.4f}")

    # Trouver le meilleur
    best = max(models, key=lambda m: m['score'])
    print(f"🏆 Meilleur modèle : {best['name']} (score: {best['score']:.4f})")

    # Entraîner et sauvegarder
    X, y = prepare_data()
    best_model_path = os.getenv(
        "AIRFLOW_MODEL_PATH",
        "/app/clean_data/best_model.pickle"
    )
    train_and_save_model(best['model'], X, y, best_model_path)
    print(f"💾 Modèle sauvegardé : {best_model_path}")

    return best['name']  # Retourne le nom du meilleur modèle

def train_best_model(task_instance):
    """
    Wrapper Airflow : récupère les scores depuis XCom
    et appelle select_and_train_best_model.
    """
    # Récupérer les scores depuis XCom
    score_lr = task_instance.xcom_pull(
        task_ids='group_Train.score_lr_task',
        key='score_lr'
    )
    score_dt = task_instance.xcom_pull(
        task_ids='group_Train.score_dt_task',
        key='score_dt'
    )
    score_rf = task_instance.xcom_pull(
        task_ids='group_Train.score_rf_task',
        key='score_rf'
    )

    print(f"📥 Scores récupérés depuis XCom :")
    print(f"   score_lr = {score_lr}")
    print(f"   score_dt = {score_dt}")
    print(f"   score_rf = {score_rf}")

    # Déléguer à la fonction pure
    best = select_and_train_best_model(score_lr, score_dt, score_rf)
    return best


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
    
# ============================================================
# DAG 1 : Collecte toutes les minutes
# ============================================================
with DAG(
    dag_id='weather_fetch_dag',
    tags=['tutorial', 'datascientest', 'weather'],
    schedule_interval=datetime.timedelta(minutes=1), 
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0),
        'retries': 1,
    },
    catchup=False
) as fetch_dag:

    fetch_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )

# ============================================================
# DAG 2 : Transformation + Entraînement 
# ============================================================
with DAG(
    dag_id='weather_dag',
    description='Récupération des données météo',
    tags=['tutorial', 'datascientest', 'weather'],
    doc_md="""
    # 🌤️ Weather ML Pipeline

## Description
Ce DAG collecte des données météorologiques depuis **OpenWeatherMap**,
les transforme et entraîne plusieurs modèles de **Machine Learning**
pour prédire les températures futures à paris, london et washington.

## Architecture du pipeline
```
fetch_weather_data
       │
       ▼
[transform_all, transform_20]
       │
       ▼
   group_Train
  ┌────┼────┐
  LR   DT   RF
  └────┼────┘
       │
       ▼
train_best_model
```
            """,
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
    