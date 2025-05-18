print("Loading cctv_batch_ingestion DAG")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta
import subprocess
import os
import re

# Configuration du logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_videos(periods, execution_date):
    base_dir = "/opt/airflow/videos/cameras"
    date_str = execution_date.strftime("%d-%m-%Y")
    for camera in os.listdir(base_dir):
        camera_dir = os.path.join(base_dir, camera)
        if not os.path.isdir(camera_dir):
            continue
        topic = f"{camera}_data"
        date_dir = f"Video_{date_str}"
        video_dir = os.path.join(camera_dir, date_dir)
        if not os.path.exists(video_dir):
            print(f"Dossier {video_dir} non trouvé")
            continue
        for period in periods:
            video_file = f"vid_{period}.mp4"
            video_path = os.path.join(video_dir, video_file)
            if os.path.exists(video_path):
                cmd = f"python /opt/airflow/scripts/ingestion.py {video_path} {topic} {period} {date_str}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                print(f"Ingestion de {video_path}: {result.stdout}")
                if result.stderr:
                    print(f"Erreur: {result.stderr}")
            else:
                print(f"Fichier {video_path} non trouvé")

def run_preprocess_videos():
    cmd = "python /opt/airflow/scripts/preprocess_video.py"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(f"Prétraitement: {result.stdout}")
    if result.stderr:
        print(f"Erreur dans preprocess_video.py: {result.stderr}")
        raise Exception(f"Erreur dans preprocess_video.py: {result.stderr}")

def check_data_presence_cmd(**context):
    date_str = context['ds']
    cmd = f"python /opt/airflow/scripts/check_data_presence.py {date_str}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(f"Vérification présence: {result.stdout}")
    if result.stderr:
        print(f"Erreur dans check_data_presence.py: {result.stderr}")
        raise Exception(f"Erreur dans check_data_presence.py: {result.stderr}")
    # Extraire le nombre de lignes depuis la sortie
    match = re.search(r"(\d+) lignes", result.stdout)
    rows = int(match.group(1)) if match else 0
    context['task_instance'].xcom_push(key='check_result', value={'rows': rows})
    return {'rows': rows}

def validate_schema_cmd(**context):
    date_str = context['ds']
    cmd = f"python /opt/airflow/scripts/validate_schema.py {date_str}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(f"Validation schéma: {result.stdout}")
    if result.stderr:
        print(f"Erreur dans validate_schema.py: {result.stderr}")
        raise Exception(f"Erreur dans validate_schema.py: {result.stderr}")

def check_data_quality_cmd(**context):
    date_str = context['ds']
    cmd = f"python /opt/airflow/scripts/data_quality.py {date_str} --strict"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(f"Vérification qualité: {result.stdout}")
    if result.stderr:
        print(f"Erreur dans data_quality.py: {result.stderr}")
        raise Exception(f"Erreur dans data_quality.py: {result.stderr}")

def branch_func(**context):
    """Décide si validate_schema et check_data_quality doivent être exécutés."""
    task_instance = context['task_instance']
    check_result = task_instance.xcom_pull(task_ids='check_minio_data_presence', key='check_result')
    if check_result and check_result.get('rows', 0) > 0:
        logger.info(f"Données disponibles ({check_result['rows']} lignes), passage à validate_schema")
        return 'validate_schema'
    else:
        logger.info("Aucune donnée disponible, passage à skip_validation")
        return 'skip_validation'

# Définir le DAG
dag = DAG(
    "cctv_batch_ingestion",
    start_date=datetime(2025, 3, 31),
    schedule="0 23 * * *",  # 23h00 chaque jour
    catchup=False,
    is_paused_upon_creation=True,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
)

# Périodes spécifiques à traiter
periods = ["09:00_10:00", "10:00_11:00"]

# Tâche 1 : Ingestion des métadonnées
task1 = PythonOperator(
    task_id="ingest_night",
    python_callable=lambda **context: ingest_videos(periods, context["execution_date"]),
    dag=dag
)

# Tâche 2 : Prétraitement des vidéos
task2 = PythonOperator(
    task_id="preprocess_videos",
    python_callable=run_preprocess_videos,
    dag=dag
)

# Tâche 3 : Vérification de la présence des données dans MinIO
task3 = PythonOperator(
    task_id="check_minio_data_presence",
    python_callable=check_data_presence_cmd,
    provide_context=True,
    dag=dag
)

# Tâche 4 : Branchement conditionnel
branch_task = BranchPythonOperator(
    task_id="branch_task",
    python_callable=branch_func,
    provide_context=True,
    dag=dag
)

# Tâche 5 : Validation du schéma des données
task5 = PythonOperator(
    task_id="validate_schema",
    python_callable=validate_schema_cmd,
    provide_context=True,
    trigger_rule="all_success",
    dag=dag
)

# Tâche 6 : Vérification de la qualité des données
task6 = PythonOperator(
    task_id="check_data_quality",
    python_callable=check_data_quality_cmd,
    provide_context=True,
    trigger_rule="all_success",
    dag=dag
)

# Tâche 7 : Sauter la validation si aucune donnée
skip_validation = DummyOperator(
    task_id="skip_validation",
    trigger_rule="all_done",
    dag=dag
)

# Définir l'ordre d'exécution
task1 >> task2 >> task3 >> branch_task
branch_task >> [task5, skip_validation]
task5 >> task6