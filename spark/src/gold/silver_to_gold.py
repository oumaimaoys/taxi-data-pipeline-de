import logging
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, hour, sum, to_date

logger = logging.getLogger(__name__)

HADOOP_AWS_PACKAGES = ",".join(
    [
        "org.apache.hadoop:hadoop-aws:3.5.0",
        "software.amazon.awssdk:bundle:2.35.4",
        "software.amazon.s3.analyticsaccelerator:analyticsaccelerator-s3:1.3.1",
    ]
)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    load_dotenv()

    bucket = os.getenv("BUCKET_NAME")
    object_key = os.getenv("SILVER_OBJECT_KEY")
    s3_endpoint = os.getenv("S3_ENDPOINT")
    access_key = os.getenv("RUSTFS_USERNAME")
    secret_key = os.getenv("RUSTFS_PASSWORD")
    missing_settings = [
        name
        for name, value in {
            "BUCKET_NAME": bucket,
            "SILVER_OBJECT_KEY": object_key,
            "S3_ENDPOINT": s3_endpoint,
            "RUSTFS_USERNAME": access_key,
            "RUSTFS_PASSWORD": secret_key,
        }.items()
        if not value
    ]
    if missing_settings:
        message = "Missing required environment variables: " + ", ".join(missing_settings)
        logger.error(message)
        raise ValueError(message)

    # Driver heap is applied when the JVM starts; SparkSession.config("spark.driver.memory") is too late.
    os.environ.setdefault("SPARK_DRIVER_MEMORY", "2g")
    os.environ.setdefault("SPARK_EXECUTOR_MEMORY", "2g")

    spark = None
    try:
        silver_path = f"s3a://{bucket}/{object_key}"
        logger.info("Starting Silver-to-Gold aggregation from %s", silver_path)
        spark = (
            SparkSession.builder
            .appName("Data Cleaning")
            .master("local[2]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.files.maxPartitionBytes", "64m")
            .config("spark.jars.packages", HADOOP_AWS_PACKAGES)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.analytics.accelerator.enabled", "false")
            .config("spark.hadoop.fs.s3a.prefetch.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .getOrCreate()
        )

        logger.info("Reading Silver Parquet data")
        silver_df = spark.read.parquet(silver_path)

        aggregations = {
            "daily_trips": (
                silver_df.withColumn("trip_date", to_date(col("trip_start_timestamp")))
                .groupBy("trip_date")
                .agg(
                    count("*").alias("total_trips"),
                    avg("trip_miles").alias("avg_distance"),
                    avg("trip_seconds").alias("avg_duration_seconds"),
                    avg("fare").alias("avg_fare"),
                    sum("trip_total").alias("total_revenue"),
                )
            ),
            "popular_pickup_locations": (
                silver_df.groupBy("pickup_community_area")
                .agg(
                    count("*").alias("total_trips"),
                    avg("trip_miles").alias("avg_distance"),
                    avg("fare").alias("avg_fare"),
                    sum("trip_total").alias("total_revenue"),
                )
                .orderBy(col("total_trips").desc())
            ),
            "popular_dropoff_locations": (
                silver_df.groupBy("dropoff_community_area")
                .agg(
                    count("*").alias("total_trips"),
                    avg("trip_miles").alias("avg_distance"),
                    avg("fare").alias("avg_fare"),
                    sum("trip_total").alias("total_revenue"),
                )
                .orderBy(col("total_trips").desc())
            ),
            "busy_hours": (
                silver_df.withColumn("hour", hour(col("trip_start_timestamp")))
                .groupBy("hour")
                .agg(
                    count("*").alias("total_trips"),
                    avg("trip_miles").alias("avg_distance"),
                    avg("fare").alias("avg_fare"),
                    sum("trip_total").alias("total_revenue"),
                )
                .orderBy(col("total_trips").desc())
            ),
            "companies_performance": (
                silver_df.groupBy("company")
                .agg(
                    count("*").alias("total_trips"),
                    avg("trip_miles").alias("avg_distance"),
                    avg("fare").alias("avg_fare"),
                    sum("trip_total").alias("total_revenue"),
                )
                .orderBy(col("total_revenue").desc())
            ),
            "profitable_taxis": (
                silver_df.groupBy("taxi_id")
                .agg(
                    count("*").alias("total_trips"),
                    avg("trip_miles").alias("avg_distance"),
                    avg("fare").alias("avg_fare"),
                    sum("trip_total").alias("total_revenue"),
                )
                .orderBy(col("total_revenue").desc())
            ),
            "popular_payment_types": (
                silver_df.groupBy("payment_type")
                .agg(
                    count("*").alias("total_trips"),
                    avg("trip_miles").alias("avg_distance"),
                    avg("fare").alias("avg_fare"),
                    sum("trip_total").alias("total_revenue"),
                )
                .orderBy(col("total_trips").desc())
            ),
        }

        for dataset_name, dataframe in aggregations.items():
            output_path = f"s3a://{bucket}/gold/{dataset_name}/"
            logger.info("Writing Gold dataset %s to %s", dataset_name, output_path)
            dataframe.write.mode("overwrite").parquet(output_path)

        logger.info("Silver-to-Gold aggregation completed successfully")
    except Exception:
        logger.exception("Silver-to-Gold aggregation failed")
        raise
    finally:
        if spark is not None:
            logger.info("Stopping Spark session")
            try:
                spark.stop()
            except Exception:
                logger.exception("Failed to stop Spark session")


if __name__ == "__main__":
    main()