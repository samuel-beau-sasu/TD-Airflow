import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump
import os


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

def select_and_train_best_model(score_lr, score_dt):
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

    print(f"📥 Scores récupérés depuis XCom :")
    print(f"   score_lr = {score_lr}")
    print(f"   score_dt = {score_dt}")

    # Déléguer à la fonction pure
    best = select_and_train_best_model(score_lr, score_dt)
    return best

def train_best_model_old(task_instance):
    """
    Récupère les scores depuis XCom et sauvegarde le meilleur modèle.
    """

    # --- 1. Définir les modèles ---
    models = [
        {
            'name': 'LinearRegression',
            'task_id': 'group_Train.score_lr_task',  # ← task_id avec groupe
            'key': 'score_lr',                        # ← clé XCom définie dans compute_lr_score_wrapper
            'model': LinearRegression()
        },
        {
            'name': 'DecisionTreeRegressor',
            'task_id': 'group_Train.score_dt_task',  # ← task_id avec groupe
            'key': 'score_dt',                        # ← clé XCom définie dans compute_dt_score_wrapper
            'model': DecisionTreeRegressor()
        },
    ]

    # --- 2. Récupérer les scores depuis XCom ---
    for m in models:
        m['score'] = task_instance.xcom_pull(
            task_ids=m['task_id'],  # ← D'où vient la valeur
            key=m['key']            # ← Quelle clé récupérer
        )
        print(f"📊 {m['name']} : {m['score']:.4f}")

    # --- 3. Trouver le meilleur modèle ---
    best = max(models, key=lambda m: m['score'])
    print(f"🏆 Meilleur modèle : {best['name']} (score: {best['score']:.4f})")

    # --- 4. Entraîner et sauvegarder ---
    X, y = prepare_data()
    best_model_path = os.getenv(
        "AIRFLOW_MODEL_PATH",
        "/app/clean_data/best_model.pickle"
    )
    train_and_save_model(best['model'], X, y, best_model_path)
    print(f"💾 Modèle sauvegardé : {best_model_path}")