# tests/test_collect.py
import sys
import os

# Définir le chemin de sortie pour les tests locaux
os.environ["AIRFLOW_RAW_FILES_PATH"] = os.path.join(
    os.path.dirname(__file__),  # tests/
    "..",                        # airflow_dst/
    "raw_files"                  # airflow_dst/raw_files/
)

# Définir le chemin de sortie pour les tests locaux
os.environ["AIRFLOW_RESULT_FILES_PATH"] = os.path.join(
    os.path.dirname(__file__),  # tests/
    "..",                        # airflow_dst/
    "clean_data"                  # airflow_dst/raw_files/
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from plugins.transform_data import transform_data_into_csv

if __name__ == "__main__":
    print("🚀 Démarrage du test...")
    
    # Passer les villes directement (sans Variable Airflow)
    test_cities = ['paris', 'london', 'washington']
    
    # Appel de la fonction AVEC les parenthèses !
    transform_data_into_csv(n_files=None, filename='data_test.csv')
    
    print(f"✅ Test réussi ! Fichier créé : /opt/airflow/clean_data")