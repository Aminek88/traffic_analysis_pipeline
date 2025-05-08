#!/bin/bash
echo "Vérification de PostgreSQL..."
until pg_isready -h postgres -p 5432 -U airflow; do
    echo "En attente de PostgreSQL..."
    sleep 2
done
echo "PostgreSQL prêt, configuration des permissions..."
echo "Création et configuration de /airflow/logs/scheduler..."
mkdir -p /airflow/logs/scheduler || { echo "Erreur : Impossible de créer /airflow/logs/scheduler"; exit 1; }
chown -R airflow:root /airflow/logs || echo "Avertissement : Échec de chown sur /airflow/logs (ignoré)"
chmod -R 775 /airflow/logs || echo "Avertissement : Échec de chmod sur /airflow/logs (ignoré)"
echo "Configuration de /opt/airflow/output..."
mkdir -p /opt/airflow/output || echo "Avertissement : Impossible de créer /opt/airflow/output"
chown -R airflow:root /opt/airflow/output 2>/dev/null || echo "Avertissement : Échec de chown sur /opt/airflow/output (ignoré)"
chmod -R 775 /opt/airflow/output 2>/dev/null || echo "Avertissement : Échec de chmod sur /opt/airflow/output (ignoré)"
echo "Initialisation de la base Airflow..."
airflow db init || { echo "Échec de airflow db init"; exit 1; }
echo "Création de l'utilisateur admin..."
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com || { echo "Échec de création utilisateur"; exit 1; }
echo "Création des buckets MinIO..."
python /opt/airflow/scripts/create_buckets.py || { echo "Échec de la création des buckets MinIO"; exit 1; }
echo "Lancement du webserver et scheduler..."
airflow webserver & airflow scheduler