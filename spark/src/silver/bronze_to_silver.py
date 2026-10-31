import json
import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import coalesce, col, lit, month, sum, year, try_to_timestamp

logger = logging.getLogger(__name__)

HADOOP_AWS_PACKAGES = ",".join(
    [
        "org.apache.hadoop:hadoop-aws:3.5.0",
        "software.amazon.awssdk:bundle:2.35.4",
    ]
)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    load_dotenv()

    bucket = os.getenv("BUCKET_NAME")
    object_key = os.getenv("BRONZE_OBJECT_KEY")
    s3_endpoint = os.getenv("S3_ENDPOINT")
    access_key = os.getenv("RUSTFS_USERNAME")
    secret_key = os.getenv("RUSTFS_PASSWORD")
    missing_settings = [
        name
        for name, value in {
            "BUCKET_NAME": bucket,
            "BRONZE_OBJECT_KEY": object_key,
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
        logger.info("Starting Bronze-to-Silver transformation for s3://%s/%s", bucket, object_key)
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
            .config("spark.hadoop.fs.s3a.input.stream.type", "classic")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .getOrCreate()
        )

        logger.info("Reading Bronze CSV and applying the source schema")
        SOURCE_COLUMNS = {
            "trip_id": "string",
            "taxi_id": "string",
            "trip_start_timestamp": "timestamp",
            "trip_end_timestamp": "timestamp",
            "trip_seconds": "int",
            "trip_miles": "double",
            "pickup_census_tract": "string",
            "dropoff_census_tract": "string",
            "pickup_community_area": "int",
            "dropoff_community_area": "int",
            "fare": "double",
            "tips": "double",
            "tolls": "double",
            "extras": "double",
            "trip_total": "double",
            "payment_type": "string",
            "company": "string",
            "pickup_centroid_latitude": "double",
            "pickup_centroid_longitude": "double",
            "pickup_centroid_location": "string",
            "dropoff_centroid_latitude": "double",
            "dropoff_centroid_longitude": "double",
            "dropoff_centroid_location": "string",
        }


        def cast_column(name, dtype):
            c = col(name)
            if dtype == "string":
                return c.alias(name)
            if dtype == "timestamp":
                return try_to_timestamp(c).alias(name)
            return c.try_cast(dtype).alias(name)


        raw_df = spark.read.csv(f"s3a://{bucket}/{object_key}", header=True) 

        bronze_df = raw_df.select(
            [cast_column(name, dtype) for name, dtype in SOURCE_COLUMNS.items()]
        )

        records_before = bronze_df.count()
        logger.info("Loaded %s Bronze records", records_before)

        logger.info("Removing duplicate rows")
        bronze_df = bronze_df.dropDuplicates()
        duplicates_removed = records_before - bronze_df.count()
        logger.info("Removed %s duplicate rows", duplicates_removed)

        logger.info("Calculating null percentages and dropping mostly-null columns")
        null_counts = bronze_df.select(
            [sum(bronze_df[c].isNull().cast("int")).alias(c) for c in bronze_df.columns]
        )
        total_rows = bronze_df.count()
        null_values = null_counts.first().asDict()
        columns_to_drop = [
            c for c, count in null_values.items()
            if total_rows > 0 and count / total_rows > 0.5
        ]
        null_percentages = {
            column: round((null_count / total_rows) * 100, 2) if total_rows > 0 else 0
            for column, null_count in null_values.items()
        }
        if columns_to_drop:
            logger.info("Dropping columns with more than 50%% null values: %s", columns_to_drop)
        bronze_df = bronze_df.drop(*columns_to_drop)

        logger.info("Filtering invalid negative trip and fare values")
        bronze_df = bronze_df.filter(
            (col("trip_miles").isNull() | (col("trip_miles") >= 0)) &
            (col("trip_seconds").isNull() | (col("trip_seconds") >= 0)) &
            (col("fare").isNull() | (col("fare") >= 0)) &
            (col("tips").isNull() | (col("tips") >= 0)) &
            (col("tolls").isNull() | (col("tolls") >= 0)) &
            (col("extras").isNull() | (col("extras") >= 0)) &
            (col("trip_total").isNull() | (col("trip_total") >= 0))
        )

        bronze_df = bronze_df.withColumn(
            "calculated_total",
            coalesce(col("fare"), lit(0)) +
            coalesce(col("tips"), lit(0)) +
            coalesce(col("tolls"), lit(0)) +
            coalesce(col("extras"), lit(0))
        )
        bronze_df = bronze_df.filter(
            ~(col("fare").isNull() &
              col("tips").isNull() &
              col("tolls").isNull() &
              col("extras").isNull() &
              col("trip_total").isNull())
        )
        total_discrepancies = bronze_df.filter(
            col("trip_total").isNotNull() &
            (spark_abs(col("trip_total") - col("calculated_total")) > 0.01)
        ).count()
        logger.info("Found %s trip-total discrepancies", total_discrepancies)

        bronze_df = bronze_df.drop(
            "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_location",
            "dropoff_centroid_location", "pickup_centroid_longitude",
            "dropoff_centroid_latitude", "dropoff_centroid_longitude"
        )
        bronze_df = bronze_df.withColumn(
            "pickup_outside_chicago", col("pickup_community_area").isNull()
        )
        bronze_df = bronze_df.withColumn(
            "dropoff_outside_chicago", col("dropoff_community_area").isNull()
        )
        bronze_df = bronze_df.withColumn("tip_mightbe_cash", col("tips").isNull())

        silver_df = (
            bronze_df
            .withColumn("year", year(col("trip_start_timestamp")))
            .withColumn("month", month(col("trip_start_timestamp")))
        )
        silver_df = silver_df.select(
            col("trip_id").cast("string"),
            col("taxi_id").cast("string"),
            col("trip_start_timestamp").cast("timestamp"),
            col("trip_seconds").cast("int"),
            col("trip_miles").cast("double"),
            col("pickup_community_area").cast("int"),
            col("dropoff_community_area").cast("int"),
            col("fare").cast("double"),
            col("tips").cast("double"),
            col("tolls").cast("double"),
            col("extras").cast("double"),
            col("trip_total").cast("double"),
            col("payment_type").cast("string"),
            col("company").cast("string"),
            col("calculated_total").cast("double"),
            col("pickup_outside_chicago").cast("boolean"),
            col("dropoff_outside_chicago").cast("boolean"),
            col("tip_mightbe_cash").cast("boolean"),
            col("year").cast("int"),
            col("month").cast("int")
        )

        records_after = silver_df.count()
        silver_path = f"s3a://{bucket}/silver/taxi_data/"
        logger.info("Writing %s cleaned records to %s", records_after, silver_path)
        silver_df.write.partitionBy("year", "month").mode("overwrite").parquet(silver_path)

        metrics = {
            "pipeline": "chicago_taxi_silver",
            "execution_timestamp": datetime.now(timezone.utc).isoformat(),
            "records_before_cleaning": records_before,
            "records_after_cleaning": records_after,
            "duplicates_removed": duplicates_removed,
            "null_percentage_per_column": null_percentages,
            "total_discrepancies": total_discrepancies,
        }
        metrics_json = json.dumps(metrics, indent=4)
        metrics_path = f"s3a://{bucket}/silver/taxi_data_metrics.json"
        logger.info("Writing data-quality metrics to %s", metrics_path)
        spark.sparkContext.parallelize([metrics_json]).saveAsTextFile(metrics_path)
        logger.info("Bronze-to-Silver transformation completed successfully")
    except Exception:
        logger.exception("Bronze-to-Silver transformation failed")
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
