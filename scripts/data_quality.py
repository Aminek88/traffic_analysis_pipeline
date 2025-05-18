import sys
import logging
from pyspark.sql import SparkSession
import pandas as pd
from io import StringIO

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_data_quality_report(date_str):
    """Génère un rapport de qualité des données pour une date donnée."""
    spark = SparkSession.builder \
        .appName("DataQualityReport") \
        .config("spark.jars", "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.261.jar") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    try:
        # Lire les données Parquet depuis MinIO
        path = f"s3a://processed/detections/date={date_str}"
        df_spark = spark.read.parquet(path)
        row_count = df_spark.count()
        logger.info(f"Nombre total de lignes pour {date_str}: {row_count}")

        if row_count == 0:
            raise ValueError("Aucune donnée trouvée pour la date spécifiée")

        # Convertir en pandas DataFrame
        data1 = df_spark.toPandas()

        # Initialiser le rapport
        report = StringIO()

        # 1. data1.info()
        report.write("\n=== Info du DataFrame ===\n")
        buffer = StringIO()
        data1.info(buf=buffer)
        report.write(buffer.getvalue())

        # 2. data1[data1.isnull().any(axis=1)]
        report.write("\n=== Lignes avec des valeurs nulles ===\n")
        null_rows = data1[data1.isnull().any(axis=1)]
        if not null_rows.empty:
            report.write(null_rows.to_string())
        else:
            report.write("Aucune ligne avec des valeurs nulles.")

        # 3. data1['class_name'].value_counts(normalize=True)
        report.write("\n\n=== Fréquence normalisée des class_name ===\n")
        class_freq = data1['class_name'].value_counts(normalize=True)
        report.write(class_freq.to_string())

        # 4. data1['video_id'].str.split('_')[0]
        report.write("\n\n=== Première partie de video_id ===\n")
        # Exemple de split basé sur ['camera2', '09-05-2025', '09:00', '10:00']
        sample_video_id = data1['video_id'].iloc[0] if not data1.empty else "camera2_09-05-2025_09:00_10:00"
        report.write(f"metadonne sur le vedio  : {sample_video_id.split('_')}\n")

        # Sauvegarder le rapport dans un seul fichier
        report_str = report.getvalue()
        report_path = f"s3a://processed/reports/date={date_str}/data_quality_report.txt"
        spark.sparkContext.parallelize([report_str]).coalesce(1).saveAsTextFile(report_path)
        logger.info(f"Rapport enregistré dans {report_path}")

        # Afficher le rapport dans les logs
        print(report_str)

        return {"status": "success", "rows": row_count}

    except Exception as e:
        logger.error(f"Échec de la génération du rapport pour {date_str}: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: python data_quality_report.py <date_str>")
        sys.exit(1)
    date_str = sys.argv[1]
    result = generate_data_quality_report(date_str)
    print(f"Génération du rapport terminée pour {date_str}: {result['rows']} lignes, Statut: {result['status']}")