import os
import json
import pandas as pd

def transform_data_into_csv(n_files=None, filename='data.csv'):
    #parent_folder = "/opt/airflow/raw_files"
    parent_folder = os.getenv(
        "AIRFLOW_RAW_FILES_PATH",  # Variable d'environnement
        "/opt/airflow/raw_files"   # Valeur par défaut dans Docker
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
        "/opt/airflow/clean_data"   # Valeur par défaut dans Docker
    )

    df.to_csv(os.path.join(result_folder, filename), index=False)