# modules/collect_data.py
import datetime
import requests
import json
import os

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
        "/opt/airflow/raw_files"   # Valeur par défaut dans Docker
    )
    
    os.makedirs(output_dir, exist_ok=True)

    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w") as f:
        json.dump(all_data, f, indent=4)

    print(f"💾 Sauvegardé : {filepath}")
    return filepath