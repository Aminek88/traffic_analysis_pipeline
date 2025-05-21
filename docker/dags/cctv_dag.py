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
                cmd = f"python /opt/airflow/scripts/Batch/ingestion.py {video_path} {topic} {period} {date_str}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                print(f"Ingestion de {video_path}: {result.stdout}")
                if result.stderr:
                    print(f"Erreur: {result.stderr}")
            else:
                print(f"Fichier {video_path} non trouvé")

def run_preprocess_videos():
    cmd = "python /opt/airflow/scripts/Batch/preprocess_video.py"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(f"Prétraitement: {result.stdout}")
    if result.returncode != 0 or ("ERROR" in result.stderr or "Exception" in result.stderr):
        print(f"Erreur dans preprocess_video.py: {result.stderr}")
        raise Exception(f"Erreur dans preprocess_video.py: {result.stderr}")
    else:
        print("Preprocess videos completed successfully")

def check_data_presence_cmd(**context):
    date_str = context['ds']  # e.g., '2025-05-19'
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%d-%m-%Y")  # e.g., '19-05-2025'
    cmd = f"python /opt/airflow/scripts/Batch/check_data_presence.py {formatted_date}"
    # Run with explicit stdout and stderr capture
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
    # Log raw outputs
    logger.info(f"Command: {cmd}")
    logger.info(f"Raw stdout: {repr(result.stdout)}")
    logger.info(f"Raw stderr: {repr(result.stderr)}")
    logger.info(f"Return code: {result.returncode}")
    print(f"Vérification présence: {result.stdout}")
    # Check for errors
    if result.returncode != 0 or ("ERROR" in result.stderr or "Exception" in result.stderr):
        logger.error(f"Erreur dans check_data_presence.py: {result.stderr}")
        raise Exception(f"Erreur dans check_data_presence.py: {result.stderr}")
    # Parse row count with flexible regex
    match = re.search(r"(\d+)\s+lignes", result.stdout, re.MULTILINE | re.IGNORECASE)
    if not match:
        # Try stderr in case output is there
        match = re.search(r"(\d+)\s+lignes", result.stderr, re.MULTILINE | re.IGNORECASE)
    rows = int(match.group(1)) if match else 0
    context['task_instance'].xcom_push(key='check_result', value={'rows': rows})
    logger.info(f"Parsed rows: {rows}")
    return {'rows': rows}

def validate_schema_cmd(**context):
    date_str = context['ds']
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%d-%m-%Y")  # e.g., '19-05-2025'
    cmd = f"python /opt/airflow/scripts/Batch/validate_schema.py {formatted_date}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(f"Validation schéma: {result.stdout}")
    if result.returncode != 0 or ("ERROR" in result.stderr or "Exception" in result.stderr):
        logger.error(f"Erreur dans validate_schema.py: {result.stderr}")
        raise Exception(f"Erreur dans validate_schema.py: {result.stderr}")
    logger.info("Schema validation completed successfully")
    return result.stdout

def check_data_quality_cmd(**context):
    date_str = context['ds']
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%d-%m-%Y")
    cmd = f"python /opt/airflow/scripts/Batch/data_quality.py {formatted_date}" 
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
    logger.info(f"Command: {cmd}")
    logger.info(f"Vérification qualité: {result.stdout}")
    logger.info(f"Raw stderr: {repr(result.stderr)}")
    logger.info(f"Return code: {result.returncode}")
    if result.returncode != 0 or ("ERROR" in result.stderr or "Exception" in result.stderr):
        logger.error(f"Erreur dans data_quality_report.py: {result.stderr}")
        raise Exception(f"Erreur dans data_quality_report.py: {result.stderr}")
    logger.info("Data quality check completed successfully")
    return result.stdout

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

### Tâche d'agrégation
def aggregate_traffic_data(**context):
    date_str = context['ds']
    formatted_date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%d-%m-%Y')
    cmd = f"python /opt/airflow/scripts/Batch/aggregate_traffic.py {formatted_date}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    logger.info(f"Aggregation: {result.stdout}")
    if result.returncode != 0 or ("ERROR" in result.stderr or "Exception" in result.stderr):
        logger.error(f"Error in aggregation: {result.stderr}")
        raise Exception(f"Error in aggregation: {result.stderr}")

# Définir le DAG
dag = DAG(
    "cctv_batch_ingestion",
    start_date=datetime(2025, 3, 31),
    schedule=None,  # 23h00 chaque jour
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

# Tâche 7 : Agrégation des données de circulation
task7 = PythonOperator(
    task_id="aggregate_traffic_data",
    python_callable=aggregate_traffic_data,
    provide_context=True,
    trigger_rule="all_done",
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
[task6, skip_validation] >> task7