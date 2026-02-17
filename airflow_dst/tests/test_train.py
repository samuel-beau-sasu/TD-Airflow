# tests/test_collect.py
import sys
import os
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor

# ✅ Définir les chemins locaux (relatifs au projet)
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Variable pour les DONNÉES
os.environ["AIRFLOW_DATA_PATH"] = os.path.join(base_dir, "clean_data", "fulldata.csv")

# Variable pour le MODÈLE
os.environ["AIRFLOW_MODEL_PATH"] = os.path.join(base_dir, "clean_data", "model.pckl")

sys.path.insert(0, base_dir)


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from plugins.train_data import compute_model_score
from plugins.train_data import train_and_save_model
from plugins.train_data import prepare_data

if __name__ == '__main__':
    
    print("🚀 Démarrage du test...")
    print(f"📂 Données    : {os.environ['AIRFLOW_DATA_PATH']}")
    print(f"💾 Modèle     : {os.environ['AIRFLOW_MODEL_PATH']}")

    X, y = prepare_data()
    print(f"✅ Données chargées : {X.shape}")

    score_lr = compute_model_score(LinearRegression(), X, y)
    score_dt = compute_model_score(DecisionTreeRegressor(), X, y)
    print(f"📊 Score LinearRegression    : {score_lr:.4f}")
    print(f"📊 Score DecisionTree        : {score_dt:.4f}")
    
    # ✅ Chemin local pour le meilleur modèle
    best_model_path = os.path.join(base_dir, "clean_data", "best_model.pickle")

    # using neg_mean_square_error
    if score_lr < score_dt:
        print("🏆 Meilleur modèle : LinearRegression")
        train_and_save_model(LinearRegression(), X, y, best_model_path)

    else:
        print("🏆 Meilleur modèle : DecisionTreeRegressor")
        train_and_save_model(DecisionTreeRegressor(), X, y, best_model_path)

    print(f"✅ Test réussi ! Modèle sauvegardé : {best_model_path}")