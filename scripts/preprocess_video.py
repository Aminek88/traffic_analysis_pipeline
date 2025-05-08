import os
import json
import logging
import time
import datetime
import gc
import psutil
import cv2
import pandas as pd
import numpy as np
from ultralytics import YOLO
import ultralytics
from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, when, lag, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType
from pyspark.sql import Window
import torch

# Réduire les logs YOLO
ultralytics.utils.LOGGER.setLevel(logging.WARNING)

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_video_to_df(video_path, period, camera, date, yolo_model, class_names):
    """Traite une vidéo et renvoie un DataFrame pandas avec les détections."""
    logger.info(f"Début du traitement de {video_path}")
    torch.set_num_threads(2)  # Utiliser tous les cœurs CPU
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        logger.error(f"Impossible d'ouvrir {video_path}")
        return None

    fps = cap.get(cv2.CAP_PROP_FPS) or 30
    frame_count = 0
    all_frames = []
    start_time = time.time()

    while cap.isOpened():
        frame_start_time = time.time()
        ret, frame = cap.read()
        if not ret:
            break

        if frame_count % 100 == 0:
            logger.info(f"Traitement de la frame {frame_count} pour la vidéo {video_path}")

        # Étape 1 : Lecture et redimensionnement
        read_resize_start = time.time()
        frame_res = cv2.resize(frame, (480, 480)) 
        read_resize_time = time.time() - read_resize_start

        # Étape 2 : Inférence YOLO
        yolo_start = time.time()
        results = yolo_model.track(frame_res, persist=True, imgsz=480)
        yolo_time = time.time() - yolo_start

        # Étape 3 : Traitement des détections
        process_start = time.time()
        detection = results[0].boxes.data
        if detection.numel() == 0:
            frame_count += 1
            continue

        frame_data = detection.cpu().numpy()
        num_cols = frame_data.shape[1]
        if frame_count % 100 == 0:
            logger.info(f"Frame {frame_count}: frame_data shape = {frame_data.shape}")

        if num_cols == 7:
            columns = ["x_min", "y_min", "x_max", "y_max", "track_id", "conf", "class"]
        elif num_cols == 6:
            columns = ["x_min", "y_min", "x_max", "y_max", "conf", "class"]
            if frame_count % 100 == 0:
                logger.warning(f"Frame {frame_count}: Tracking failed, no track_id.")
        else:
            logger.error(f"Frame {frame_count}: Unexpected number of columns ({num_cols}).")
            frame_count += 1
            continue

        # Stocker les détections directement comme liste de dictionnaires
        detections = [
            {
                "x_min": float(x[0]),
                "y_min": float(x[1]),
                "x_max": float(x[2]),
                "y_max": float(x[3]),
                "track_id": float(x[4]) if num_cols == 7 else float("nan"),
                "conf": float(x[5]),
                "class_name": class_names[int(x[6] if num_cols == 7 else x[5])],
                "frame_id": frame_count
            }
            for x in frame_data
        ]

        all_frames.append({
            "video_id": f"{camera}_{date}_{period}",
            "timestamp": datetime.datetime.now().isoformat(),
            "frame_id": frame_count,
            "detections": detections
        })
        process_time = time.time() - process_start

        # Libérer la mémoire
        gc.collect()

        if frame_count % 100 == 0:
            logger.info(f"Frame {frame_count}: Temps lecture/redimensionnement = {read_resize_time:.3f}s")
            logger.info(f"Frame {frame_count}: Temps YOLO = {yolo_time:.3f}s")
            logger.info(f"Frame {frame_count}: Temps traitement détections = {process_time:.3f}s")
            logger.info(f"Frame {frame_count}: Temps total frame = {time.time() - frame_start_time:.3f}s")
            process = psutil.Process()
            mem_info = process.memory_info()
            logger.info(f"Frame {frame_count}: Utilisation mémoire = {mem_info.rss / 1024 / 1024:.2f} MB")

        frame_count += 1

    cap.release()
    logger.info(f"Traitement terminé pour {video_path} - {frame_count} frames")
    process = psutil.Process()
    mem_info = process.memory_info()
    logger.info(f"Utilisation mémoire après traitement : {mem_info.rss / 1024 / 1024:.2f} MB")
    return pd.DataFrame(all_frames)

def preprocess_videos():
    """Prétraitement des vidéos : lecture Kafka, traitement YOLO, sauvegarde en Parquet dans MinIO."""
    model_path = "/opt/airflow/scripts/yolo11n.pt"
    if not os.path.exists(model_path):
        logger.error(f"Le fichier modèle {model_path} n'existe pas dans le conteneur")
        raise FileNotFoundError(f"Modèle YOLOv11n non trouvé à {model_path}")
    
    logger.info(f"Chargement du modèle YOLO depuis {model_path}")
    yolo_model = YOLO(model_path)
    class_names = yolo_model.names

    spark = SparkSession.builder \
        .appName("VideoProcessing") \
        .config("spark.jars", "/opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/airflow/jars/kafka-clients-3.4.1.jar,/opt/airflow/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,/opt/airflow/jars/commons-pool2-2.11.1.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.261.jar") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.default.parallelism", "16") \
        .getOrCreate()

    try:
        spark._jvm.org.apache.spark.sql.kafka010.KafkaSourceProvider
        logger.info("KafkaSourceProvider chargé avec succès.")
    except Exception as e:
        logger.error(f"Erreur lors du chargement de KafkaSourceProvider : {str(e)}")
        raise

    kafka_schema = StructType([
        StructField("video_path", StringType(), True),
        StructField("period", StringType(), True),
        StructField("camera", StringType(), True),
        StructField("date", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("status", StringType(), True)
    ])

    detection_schema = StructType([
        StructField("video_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("frame_id", IntegerType(), True),
        StructField("detections", ArrayType(StructType([
            StructField("x_min", FloatType(), True),
            StructField("y_min", FloatType(), True),
            StructField("x_max", FloatType(), True),
            StructField("y_max", FloatType(), True),
            StructField("track_id", FloatType(), True),
            StructField("conf", FloatType(), True),
            StructField("class_name", StringType(), True)
        ])), True)
    ])

    logger.info("Tentative de lecture depuis Kafka : kafka:9092, topics camera1_data, camera2_data")
    data_batch = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "camera1_data,camera2_data") \
        .option("startingOffsets", "earliest") \
        .load()
    
    logger.info("Schéma du DataFrame Kafka :")
    data_batch.printSchema()

    parsed_data = data_batch.select(
        from_json(col("value").cast("string"), kafka_schema).alias("data")
    ).select(
        "data.video_path", "data.period", "data.camera", "data.date"
    ).filter(col("data.status") == "ready_for_processing").distinct()

    logger.info("Schéma du DataFrame parsé :")
    parsed_data.printSchema()

    metadata_path = f"s3a://metadata/{datetime.datetime.now().strftime('%Y-%m-%d')}/kafka_metadata"
    parsed_data.write.json(metadata_path, mode="overwrite")
    logger.info(f"Métadonnées Kafka sauvegardées dans {metadata_path}")

    for row in parsed_data.collect():
        video_path = row["video_path"]
        period = row["period"]
        camera = row["camera"]
        date = row["date"]
        logger.info(f"Traitement de {video_path}")

        pandas_df = process_video_to_df(video_path, period, camera, date, yolo_model, class_names)
        if pandas_df is None or pandas_df.empty:
            logger.warning(f"Aucune donnée pour {video_path}")
            continue

        spark_df = spark.createDataFrame(pandas_df, schema=detection_schema)

        transform_start = time.time()
        detection_df = spark_df.select(
            col("video_id"),
            col("timestamp"),
            col("frame_id"),
            explode(col("detections")).alias("detection")
        ).select(
            col("video_id"),
            col("timestamp"),
            col("frame_id"),
            col("detection.x_min").alias("x_min"),
            col("detection.y_min").alias("y_min"),
            col("detection.x_max").alias("x_max"),
            col("detection.y_max").alias("y_max"),
            col("detection.track_id").alias("track_id"),
            col("detection.conf").alias("confiance"),
            col("detection.class_name").alias("class_name"),
            lit(date).alias("date"),
            lit(camera).alias("camera"),
            lit(period).alias("period")
        )

        detection_df = detection_df.withColumn("x_centre", (col("x_max") + col("x_min")) / 2) \
                                   .withColumn("y_centre", (col("y_max") + col("y_min")) / 2)
        window_spec = Window.partitionBy("track_id").orderBy("frame_id")
        detection_df = detection_df.withColumn("x_delta", col("x_centre") - lag("x_centre", 1, 0).over(window_spec)) \
                                   .withColumn("y_delta", col("y_centre") - lag("y_centre", 1, 0).over(window_spec)) \
                                   .withColumn("x_velocity", col("x_delta") / 30.0) \
                                   .withColumn("y_velocity", col("y_delta") / 30.0) \
                                   .drop("x_centre", "y_centre")

        detection_df = detection_df.filter(col("track_id").isNotNull())

        # Réordonner par frame_id pour un ordre chronologique
        detection_df = detection_df.orderBy("frame_id")

        transform_time = time.time() - transform_start
        logger.info(f"Temps transformations Spark pour {video_path}: {transform_time:.3f}s")

        row_count = detection_df.count()
        logger.info(f"Nombre de lignes pour {video_path}: {row_count}")

        output_path = f"s3a://processed/detections"
        detection_df.write.partitionBy("date", "camera", "period").parquet(output_path, mode="append")
        logger.info(f"Résultats sauvegardés dans {output_path}/date={date}/camera={camera}/period={period}")

    spark.stop()

if __name__ == "__main__":
    preprocess_videos()