import sys
import os
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor

# Définir les chemins locaux
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
os.environ["AIRFLOW_DATA_PATH"] = os.path.join(base_dir, "clean_data", "fulldata.csv")
os.environ["AIRFLOW_MODEL_PATH"] = os.path.join(base_dir, "clean_data", "best_model.pickle")

sys.path.insert(0, base_dir)

from plugins.train_data import (
    compute_model_score,
    train_and_save_model,
    prepare_data,
    select_and_train_best_model  # ← Fonction pure, testable !
)

if __name__ == '__main__':
    print("🚀 Démarrage du test...")
    print(f"📂 Données : {os.environ['AIRFLOW_DATA_PATH']}")
    print(f"💾 Modèle  : {os.environ['AIRFLOW_MODEL_PATH']}")

    # 1. Préparer les données
    X, y = prepare_data()
    print(f"✅ Données chargées : {X.shape}")

    # 2. Calculer les scores (simulation des tâches Airflow)
    score_lr = compute_model_score(LinearRegression(), X, y)
    score_dt = compute_model_score(DecisionTreeRegressor(), X, y)
    print(f"📊 Score LR : {score_lr:.4f}")
    print(f"📊 Score DT : {score_dt:.4f}")

    # 3. Tester select_and_train_best_model directement
    best = select_and_train_best_model(score_lr, score_dt)

    print(f"✅ Test réussi ! Meilleur modèle : {best}")
    print(f"💾 Modèle sauvegardé : {os.environ['AIRFLOW_MODEL_PATH']}")