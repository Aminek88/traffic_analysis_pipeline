from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_schema(date_str):
    """Valide le schéma des fichiers Parquet pour une date donnée dans MinIO."""
    try:
        # Configurer Spark avec la même configuration que preprocess_video.py
        spark = SparkSession.builder \
            .appName("ValidateMinIOSchema") \
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

        # Définir le schéma attendu (basé sur preprocess_video.py)
        expected_schema = StructType([
            StructField("video_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("frame_id", IntegerType(), True),
            StructField("x_min", FloatType(), True),
            StructField("y_min", FloatType(), True),
            StructField("x_max", FloatType(), True),
            StructField("y_max", FloatType(), True),
            StructField("track_id", FloatType(), True),
            StructField("confiance", FloatType(), True),
            StructField("class_name", StringType(), True),
            StructField("x_delta", DoubleType(), True),
            StructField("y_delta", DoubleType(), True),
            StructField("x_velocity", DoubleType(), True),
            StructField("y_velocity", DoubleType(), True),
            StructField("camera", StringType(), True),
            StructField("period", StringType(), True)
        ])

        # Lire les fichiers Parquet
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

        # Vérifier le schéma
        actual_schema = df.schema
        if actual_schema != expected_schema:
            logger.error(f"Échec de la validation du schéma pour {date_str}. Attendu: {expected_schema}, Trouvé: {actual_schema}")
            raise ValueError(f"Schéma invalide pour {date_str}")

        # Vérifier les valeurs des colonnes de partition
        expected_cameras = {"camera1", "camera2"}
        expected_periods = {"09:00_10:00", "10:00_11:00"}
        cameras_present = set(df.select("camera").distinct().rdd.map(lambda x: x[0]).collect())
        periods_present = set(df.select("period").distinct().rdd.map(lambda x: x[0]).collect())

        if not cameras_present.issubset(expected_cameras):
            logger.error(f"Caméras invalides trouvées: {cameras_present - expected_cameras}")
            raise ValueError(f"Caméras invalides pour {date_str}")
        if not periods_present.issubset(expected_periods):
            logger.error(f"Périodes invalides trouvées: {periods_present - expected_periods}")
            raise ValueError(f"Périodes invalides pour {date_str}")

        # Vérifier les valeurs nulles pour les colonnes clés
        null_checks = [
            ("track_id", df.filter(df.track_id.isNull()).count()),
            ("class_name", df.filter(df.class_name.isNull()).count()),
            ("camera", df.filter(df.camera.isNull()).count()),
            ("period", df.filter(df.period.isNull()).count())
        ]
        for column, null_count in null_checks:
            if null_count > 0:
                logger.error(f"{null_count} valeurs nulles trouvées dans la colonne clé {column}")
                raise ValueError(f"Valeurs nulles dans {column} pour {date_str}")

        # Log succès
        logger.info(f"Validation du schéma terminée pour {date_str}: {row_count} lignes, Caméras: {cameras_present}, Périodes: {periods_present}, Statut: schema compatible")
        
        spark.stop()
        return {
            "status": "success",
            "rows": row_count,
            "cameras": list(cameras_present),
            "periods": list(periods_present),
            "schema_valid": True
        }

    except Exception as e:
        logger.error(f"Échec de la validation du schéma pour {date_str}: {str(e)}")
        raise

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python validate_schema.py <date_str>")
        sys.exit(1)
    validate_schema(sys.argv[1])