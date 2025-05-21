from pyspark.sql import SparkSession
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_data_presence(date_str):
    """Vérifie la présence de données Parquet pour une date donnée dans MinIO."""
    try:
        # Configurer Spark avec la même configuration que preprocess_video.py
        spark = SparkSession.builder \
            .appName("CheckMinIODataPresence") \
            .config("spark.jars", "/opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/airflow/jars/kafka-clients-3.4.1.jar,/opt/airflow/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,/opt/airflow/jars/commons-pool2-2.11.1.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.261.jar") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.sql.shuffle.partitions", "16") \
            .config("spark.default.parallelism", "16") \
            .getOrCreate()

        # Vérifier l'existence des fichiers
        path = f"s3a://processed/detections/date={date_str}"
        try:
            df = spark.read.parquet(path)
        except Exception as e:
            logger.error(f"Aucun fichier trouvé dans {path}: {str(e)}")
            raise ValueError(f"Aucun fichier Parquet pour {date_str}")

        # Vérifier que les données ne sont pas vides
        row_count = df.count()
        if row_count == 0:
            logger.error(f"Les fichiers dans {path} sont vides")
            raise ValueError(f"Données vides pour {date_str}")

        # Vérifier la présence des caméras et périodes attendues
        expected_cameras = {"camera1", "camera2"}
        expected_periods = {"09:00_10:00", "10:00_11:00"}
        cameras_present = set(df.select("camera").distinct().rdd.map(lambda x: x[0]).collect())
        periods_present = set(df.select("period").distinct().rdd.map(lambda x: x[0]).collect())

        missing_cameras = expected_cameras - cameras_present
        missing_periods = expected_periods - periods_present

        # Log avertissement pour données partielles
        status = "success"
        if missing_cameras or missing_periods:
            logger.warning(f"Données partielles pour {date_str}. Caméras manquantes: {missing_cameras}, Périodes manquantes: {missing_periods}")
            status = "partial"

        # Log succès
        logger.info(f"Vérification de présence terminée pour {date_str}: {row_count} lignes, Caméras: {cameras_present}, Périodes: {periods_present}, Statut: {status}")
        spark.stop()
        return {
            "status": status,
            "rows": row_count,
            "cameras": list(cameras_present),
            "periods": list(periods_present),
            "missing_cameras": list(missing_cameras),
            "missing_periods": list(missing_periods)
        }

    except Exception as e:
        logger.error(f"Échec de la vérification de présence pour {date_str}: {str(e)}")
        raise

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python check_data_presence.py <date_str>")
        sys.exit(1)
    check_data_presence(sys.argv[1])