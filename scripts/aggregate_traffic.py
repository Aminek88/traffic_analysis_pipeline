from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, avg, hour, concat, lit
from datetime import datetime
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def aggregate_traffic_data(formatted_date):
    spark = SparkSession.builder \
        .appName("TrafficAggregation") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.mapreduce.outputcommitter.factory.class", "org.apache.hadoop.mapreduce.lib.output.DirectOutputCommitter") \
        .getOrCreate()

    try:
        s3_path = f"s3a://processed/detections/date={formatted_date}/"
        df = spark.read.parquet(s3_path)
        logger.info(f"Read {df.count()} rows from {s3_path}")

        # Print schema for debugging
        logger.info("DataFrame schema:")
        df.printSchema()

        # Add date column from formatted_date
        df = df.withColumn("date", lit(formatted_date))

        # Use existing period from Parquet, filter valid periods
        df = df.filter(col("period").isin(["09:00_10:00", "10:00_11:00"]))

        # Group by date, camera, period, class_name
        agg_df = df.groupBy("date", "camera", "period", "class_name").agg(
            countDistinct("track_id").alias("vehicle_count"),
            avg("x_velocity").alias("avg_x_velocity"),
            avg("y_velocity").alias("avg_y_velocity"),
            countDistinct("frame_id").alias("frame_count")
        )
        agg_df = agg_df.withColumn("density_score", col("vehicle_count") / col("frame_count")) \
                       .withColumn("congestion_score", col("vehicle_count") / col("avg_x_velocity")) \
                       .withColumn("timestamp", lit(datetime.now()))

        # Write to PostgreSQL
        agg_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/traffic_db") \
            .option("dbtable", "traffic_summary") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        logger.info(f"Inserted rows for {formatted_date} into traffic_summary")

    except Exception as e:
        logger.error(f"Error in aggregation: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: aggregate_traffic.py <formatted_date>")
        sys.exit(1)
    aggregate_traffic_data(sys.argv[1])