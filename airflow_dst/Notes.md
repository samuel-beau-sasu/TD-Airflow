# Airflow

# Créer le fichier
docker-compose.yaml et pas docker-compose.yml

# Airflow nécessite la création de volumes pour simplifier l'échange de fichiers depuis la machine hôte vers les containers

mkdir ./dags ./logs ./plugins

sudo chmod -R 777 logs/
sudo chmod -R 777 dags/
sudo chmod -R 777 plugins/

# créer un fichier de variables d'environnement pour le fichier docker-compose.

echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

# initialiser la base de données pour l'utilisation d'Airflow.

docker-compose up airflow-init

# Exécutez la commande suivante pour lancer les containers
docker-compose up -d

docker-compose down

# vérifier l'état des conteneurs
docker container ls

# Demarrer l'interface graphique
18.200.48.46:8080

Username: airflow
Password: airflow

# Le terminal d'Airflow
wget https://dst-de.s3.eu-west-3.amazonaws.com/airflow_avance_fr/docker-compose/airflow.sh
chmod +x airflow.sh

# test email 

docker-compose logs airflow-scheduler | grep -i email
docker-compose logs airflow-scheduler | grep -i smtp