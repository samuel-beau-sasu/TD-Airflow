# tests/test_collect.py
import sys
import os


# Définir le chemin de sortie pour les tests locaux
os.environ["AIRFLOW_RAW_FILES_PATH"] = os.path.join(
    os.path.dirname(__file__),  # tests/
    "..",                        # airflow_dst/
    "raw_files"                  # airflow_dst/raw_files/
)


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from plugins.collect_data import fetch_weather_data

if __name__ == "__main__":
    print("🚀 Démarrage du test...")
    
    # Passer les villes directement (sans Variable Airflow)
    test_cities = ['paris', 'london', 'washington']
    
    # Appel de la fonction AVEC les parenthèses !
    data_filepath = fetch_weather_data(cities=test_cities)
    
    print(f"✅ Test réussi ! Fichier créé : {data_filepath}")