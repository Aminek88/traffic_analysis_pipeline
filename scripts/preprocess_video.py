print('#############################################################')
import os
import logging
import time
import datetime
import gc
import psutil
import cv2
import pandas as pd
import numpy as np
from ultralytics import YOLO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, lag, lit, rand, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType
from pyspark.sql import Window
import torch

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_video_to_df(video_path, period, camera, date, yolo_model, class_names, batch_size=16):
    """Traite une vidéo par batch et renvoie un DataFrame pandas avec les détections."""
    logger.info(f"Processing video: {video_path}")
    torch.set_num_threads(os.cpu_count())
    logger.info(f"PyTorch threads set to: {os.cpu_count()}")

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        logger.error(f"Failed to open video: {video_path}")
        return None

    fps = cap.get(cv2.CAP_PROP_FPS) or 30
    frame_count = 0
    all_frames = []
    frames_batch = []
    frame_ids = []

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret and not frames_batch:
            break

        if ret:
            frame_res = cv2.resize(frame, (480, 480))
            frames_batch.append(frame_res)
            frame_ids.append(frame_count)

        if len(frames_batch) == batch_size or (not ret and frames_batch):
            batch_start_time = time.time()
            yolo_start = time.time()
            results = yolo_model.track(frames_batch, persist=True, imgsz=480)
            yolo_time = time.time() - yolo_start

            process_start = time.time()
            for i, (result, frame_id) in enumerate(zip(results, frame_ids)):
                detection = result.boxes.data
                if detection.numel() == 0:
                    continue

                frame_data = detection.cpu().numpy()
                num_cols = frame_data.shape[1]

                if num_cols == 7:
                    columns = ["x_min", "y_min", "x_max", "y_max", "track_id", "conf", "class"]
                elif num_cols == 6:
                    columns = ["x_min", "y_min", "x_max", "y_max", "conf", "class"]
                    logger.warning(f"Frame {frame_id}: Tracking failed, no track_id.")
                else:
                    logger.error(f"Frame {frame_id}: Unexpected number of columns ({num_cols}).")
                    continue

                detections = [
                    {
                        "x_min": float(x[0]),
                        "y_min": float(x[1]),
                        "x_max": float(x[2]),
                        "y_max": float(x[3]),
                        "track_id": float(x[4]) if num_cols == 7 else float("nan"),
                        "conf": float(x[5]),
                        "class_name": class_names[int(x[6] if num_cols == 7 else x[5])],
                        "frame_id": frame_id
                    }
                    for x in frame_data
                ]

                all_frames.append({
                    "video_id": f"{camera}_{date}_{period}",
                    "timestamp": datetime.datetime.now().isoformat(),
                    "frame_id": frame_id,
                    "detections": detections
                })

            process_time = time.time() - process_start
            frames_batch = []
            gc.collect()

            if frame_count % 100 == 0:
                logger.info(f"Batch at frame {frame_count}: YOLO time = {yolo_time:.3f}s")
                logger.info(f"Batch at frame {frame_count}: Detection processing time = {process_time:.3f}s")
                logger.info(f"Batch at frame {frame_count}: Total batch time = {time.time() - batch_start_time:.3f}s")
                process = psutil.Process()
                mem_info = process.memory_info()
                logger.info(f"Batch at frame {frame_count}: Memory usage = {mem_info.rss / 1024 / 1024:.2f} MB")

            frame_ids = []

        if ret:
            frame_count += 1

    cap.release()
    logger.info(f"Completed processing {video_path} - {frame_count} frames")
    process = psutil.Process()
    mem_info = process.memory_info()
    logger.info(f"Memory usage after processing: {mem_info.rss / 1024 / 1024:.2f} MB")
    return pd.DataFrame(all_frames)

def preprocess_videos():
    """Prétraitement des vidéos : lecture Kafka, traitement YOLO, sauvegarde en Parquet dans MinIO."""
    model_path = "/opt/airflow/scripts/yolo11n.pt"
    if not os.path.exists(model_path):
        logger.error(f"Model file {model_path} not found in container")
        raise FileNotFoundError(f"YOLOv11n model not found at {model_path}")

    logger.info(f"Loading YOLO model from {model_path}")
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
        logger.info("KafkaSourceProvider loaded successfully")
    except Exception as e:
        logger.error(f"Error loading KafkaSourceProvider: {str(e)}")
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

    logger.info("Reading from Kafka: kafka:9092, topics camera1_data, camera2_data")
    try:
        data_batch = spark.read.format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "camera1_data,camera2_data") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
    except Exception as e:
        logger.error(f"Error reading from Kafka: {str(e)}")
        raise

    logger.info("Kafka DataFrame schema:")
    data_batch.printSchema()

    parsed_data = data_batch.select(
        from_json(col("value").cast("string"), kafka_schema).alias("data")
    ).select(
        "data.video_path", "data.period", "data.camera", "data.date"
    ).filter(col("data.status") == "ready_for_processing").distinct()

    logger.info("Parsed DataFrame schema:")
    parsed_data.printSchema()

    metadata_path = f"s3a://metadata/{datetime.datetime.now().strftime('%Y-%m-%d')}/kafka_metadata"
    parsed_data.cache()
    row_count = parsed_data.count()
    logger.info(f"Number of rows in parsed_data: {row_count}")
    if row_count == 0:
        logger.warning("No valid data found in Kafka. Stopping processing.")
        spark.stop()
        return

    parsed_data.write.json(metadata_path, mode="overwrite")
    logger.info(f"Kafka metadata saved to {metadata_path}")

    for row in parsed_data.collect():
        video_path = row["video_path"]
        period = row["period"]
        camera = row["camera"]
        date = row["date"]
        logger.info(f"Processing video: {video_path}")

        pandas_df = process_video_to_df(video_path, period, camera, date, yolo_model, class_names)
        if pandas_df is None or pandas_df.empty:
            logger.warning(f"No data for {video_path}")
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

        # Calcul des vélocités avec correction pour le premier frame
        detection_df = detection_df.withColumn("x_centre", (col("x_max") + col("x_min")) / 2) \
                                   .withColumn("y_centre", (col("y_max") + col("y_min")) / 2)
        window_spec = Window.partitionBy("track_id").orderBy("frame_id")
        detection_df = detection_df.withColumn("x_delta", 
            when(lag("x_centre", 1).over(window_spec).isNotNull(),
                 col("x_centre") - lag("x_centre", 1).over(window_spec)).otherwise(0.0)) \
                                   .withColumn("y_delta", 
            when(lag("y_centre", 1).over(window_spec).isNotNull(),
                 col("y_centre") - lag("y_centre", 1).over(window_spec)).otherwise(0.0)) \
                                   .withColumn("x_velocity", col("x_delta") / 30.0) \
                                   .withColumn("y_velocity", col("y_delta") / 30.0) \
                                   .drop("x_centre", "y_centre")

        # Filtrer les détections sans track_id
        detection_df = detection_df.filter(col("track_id").isNotNull())

        # Ajouter une colonne aléatoire pour mélanger les track_id au sein de chaque frame_id
        detection_df = detection_df.withColumn("temp_rand", rand())

        # Tri global par frame_id avec mélange aléatoire
        detection_df = detection_df.orderBy("frame_id", "temp_rand").drop("temp_rand").cache()

        # Débogage : vérifier l'ordre global
        logger.info(f"Order after global sort for {video_path}")
        detection_df.select("frame_id", "track_id", "class_name").show(50, truncate=False)

        # Débogage : vérifier l'ordre pour frame_id 0
        logger.info(f"Order for frame_id 0 for {video_path}")
        detection_df.filter(col("frame_id") == 0) \
                    .select("frame_id", "track_id", "class_name") \
                    .show(10, truncate=False)

        # Débogage : vérifier les derniers frame_id
        logger.info(f"Order for last frame_ids for {video_path}")
        detection_df.select("frame_id", "track_id", "class_name").orderBy(col("frame_id").desc()).show(20, truncate=False)

        # Forcer un seul fichier par partition
        detection_df = detection_df.coalesce(1)

        transform_time = time.time() - transform_start
        logger.info(f"Spark transformation time for {video_path}: {transform_time:.3f}s")

        row_count = detection_df.count()
        logger.info(f"Number of rows for {video_path}: {row_count}")

        output_path = f"s3a://processed/detections"
        detection_df.write.partitionBy("date", "camera", "period").parquet(output_path, mode="overwrite")
        logger.info(f"Results saved to {output_path}/date={date}/camera={camera}/period={period}")

        # Vider le cache
        detection_df.unpersist()

    spark.stop()

if __name__ == "__main__":
    preprocess_videos()