import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import json
from datetime import datetime, timezone
from pyspark.sql.functions import col, sum, count, avg, to_date, hour

load_dotenv()

BUCKET = os.getenv("BUCKET_NAME")
OBJECT_KEY = os.getenv("SILVER_OBJECT_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
ACCESS_KEY = os.getenv("RUSTFS_USERNAME")
SECRET_KEY = os.getenv("RUSTFS_PASSWORD")

# Driver heap is applied when the JVM starts; SparkSession.config("spark.driver.memory") is too late.
os.environ.setdefault("SPARK_DRIVER_MEMORY", "2g")
os.environ.setdefault("SPARK_EXECUTOR_MEMORY", "2g")

if not BUCKET or not OBJECT_KEY:
    raise SystemExit("BUCKET_NAME and SILVER_OBJECT_KEY must be set")

HADOOP_AWS_PACKAGES = ",".join(
    [
        "org.apache.hadoop:hadoop-aws:3.5.0",
        "software.amazon.awssdk:bundle:2.35.4",
        "software.amazon.s3.analyticsaccelerator:analyticsaccelerator-s3:1.3.1",
    ]
)

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
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
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

# read silver data from S3
SILVER_PATH = f"s3a://{BUCKET}/{OBJECT_KEY}"

silver_df = spark.read.parquet(SILVER_PATH)

# daily trips metrics
daily_trips = (
    silver_df
    .withColumn(
        "trip_date",
        to_date(col("trip_start_timestamp"))
    )
    .groupBy("trip_date")
    .agg(
        count("*").alias("total_trips"),
        avg("trip_miles").alias("avg_distance"),
        avg("trip_seconds").alias("avg_duration_seconds"),
        avg("fare").alias("avg_fare"),
        sum("trip_total").alias("total_revenue")
    )
)

daily_trips.write.mode("overwrite").parquet(f"s3a://{BUCKET}/gold/daily_trips/")

# popular pickup location
popular_pickup_locations = (
    silver_df
    .groupBy("pickup_community_area")
    .agg(
        count("*").alias("total_trips"),
        avg("trip_miles").alias("avg_distance"),
        avg("fare").alias("avg_fare"),
        sum("trip_total").alias("total_revenue")
    )
    .orderBy(col("total_trips").desc())
)

        
popular_pickup_locations.write.mode("overwrite").parquet(f"s3a://{BUCKET}/gold/popular_pickup_locations/")

# popular dropoff location
popular_dropoff_locations = (
    silver_df
    .groupBy("dropoff_community_area")
    .agg(
        count("*").alias("total_trips"),
        avg("trip_miles").alias("avg_distance"),
        avg("fare").alias("avg_fare"),
        sum("trip_total").alias("total_revenue")
    )
    .orderBy(col("total_trips").desc())
)
popular_dropoff_locations.write.mode("overwrite").parquet(f"s3a://{BUCKET}/gold/popular_dropoff_locations/")

# most busy hours
busy_hours = (
    silver_df
    .withColumn("hour", hour(col("trip_start_timestamp")))
    .groupBy("hour")
    .agg(
        count("*").alias("total_trips"),
        avg("trip_miles").alias("avg_distance"),
        avg("fare").alias("avg_fare"),
        sum("trip_total").alias("total_revenue")
    )
    .orderBy(col("total_trips").desc())
)

busy_hours.write.mode("overwrite").parquet(f"s3a://{BUCKET}/gold/busy_hours/")

# taxi companies performance
companies_performance = (
    silver_df
    .groupBy("company")
    .agg(
        count("*").alias("total_trips"),
        avg("trip_miles").alias("avg_distance"),
        avg("fare").alias("avg_fare"),
        sum("trip_total").alias("total_revenue")
    )
    .orderBy(col("total_revenue").desc())
)

companies_performance.write.mode("overwrite").parquet(f"s3a://{BUCKET}/gold/companies_performance/")

# most profitable taxis
profitable_taxis = (
    silver_df
    .groupBy("taxi_id")
    .agg(
        count("*").alias("total_trips"),
        avg("trip_miles").alias("avg_distance"),
        avg("fare").alias("avg_fare"),
        sum("trip_total").alias("total_revenue")
    )
    .orderBy(col("total_revenue").desc())
)

profitable_taxis.write.mode("overwrite").parquet(f"s3a://{BUCKET}/gold/profitable_taxis/")

# most popular payment types
popular_payment_types = (
    silver_df
    .groupBy("payment_type")
    .agg(
        count("*").alias("total_trips"),
        avg("trip_miles").alias("avg_distance"),
        avg("fare").alias("avg_fare"),
        sum("trip_total").alias("total_revenue")
    )
    .orderBy(col("total_trips").desc())
)

popular_payment_types.write.mode("overwrite").parquet(f"s3a://{BUCKET}/gold/popular_payment_types/")



spark.stop()