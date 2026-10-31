from numbers import Number
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import json
from datetime import datetime, timezone
from pyspark.sql.functions import col, sum, year, month, coalesce, lit

load_dotenv()

BUCKET = os.getenv("BUCKET_NAME")
OBJECT_KEY = os.getenv("BRONZE_OBJECT_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
ACCESS_KEY = os.getenv("RUSTFS_USERNAME")
SECRET_KEY = os.getenv("RUSTFS_PASSWORD")

if not BUCKET or not OBJECT_KEY:
    raise SystemExit("BUCKET_NAME and BRONZE_OBJECT_KEY must be set")

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
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars.packages", HADOOP_AWS_PACKAGES)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .getOrCreate()
)

# enforce schema
source_schema = "trip_id STRING, taxi_id STRING, trip_start_timestamp TIMESTAMP, trip_end_timestamp TIMESTAMP, trip_seconds INT, trip_miles DOUBLE, pickup_census_tract STRING, dropoff_census_tract STRING, pickup_community_area INT, dropoff_community_area INT, fare DOUBLE, tips DOUBLE, tolls DOUBLE, extras DOUBLE, trip_total DOUBLE, payment_type STRING, company STRING, pickup_latitude DOUBLE, pickup_longitude DOUBLE, dropoff_latitude DOUBLE, dropoff_longitude DOUBLE"
bronze_df = spark.read.csv(f"s3a://{BUCKET}/{OBJECT_KEY}", header=True, schema=source_schema)
records_before = bronze_df.count()

# drop duplicates
bronze_df = bronze_df.dropDuplicates()

duplicates_removed = records_before - bronze_df.count()

#  drop columns that are mostly null
# how many null values are in each column
null_counts = bronze_df.select([sum(bronze_df[col].isNull().cast("int")).alias(col) for col in bronze_df.columns])

#percentage of null values in each column
total_rows = bronze_df.count()

null_values = null_counts.first().asDict()

columns_to_drop = [
    c for c, count in null_values.items()
    if total_rows > 0 and count / total_rows > 0.5
]

null_percentages = {
    column: round((null_count / records_before) * 100, 2)
    if records_before > 0 else 0
    for column, null_count in null_counts.items()
}

bronze_df = bronze_df.drop(*columns_to_drop)

# validate that all non null numbers are positive
bronze_df = bronze_df.filter(
    (col("trip_miles").isNull() | (col("trip_miles") >= 0)) &
    (col("trip_seconds").isNull() | (col("trip_seconds") >= 0)) &
    (col("fare").isNull() | (col("fare") >= 0)) &
    (col("tips").isNull() | (col("tips") >= 0)) &
    (col("tolls").isNull() | (col("tolls") >= 0)) &
    (col("extras").isNull() | (col("extras") >= 0)) &
    (col("trip_total").isNull() | (col("trip_total") >= 0))
)

# make sure trip_total is equal to fare + tips + tolls + extras
# recalculate trip_total, is any is null we put 0 in its place
bronze_df = bronze_df.withColumn(
    "calculated_total",
    coalesce(col("fare"), lit(0)) +
    coalesce(col("tips"), lit(0)) +
    coalesce(col("tolls"), lit(0)) +
    coalesce(col("extras"), lit(0))
)

# drop rows where fare, tips, tolls, extras, or trip_total are all null
bronze_df = bronze_df.filter(
    ~(col("fare").isNull() &
      col("tips").isNull() &
      col("tolls").isNull() &
      col("extras").isNull() &
      col("trip_total").isNull())
)

# check for discrepancies between trip_total and calculated_total
total_discrepancies = bronze_df.filter(
    col("trip_total").isNotNull() &
    (abs(col("trip_total") - col("calculated_total")) > 0.01)
).count()

# drop unnecessary columns
bronze_df = bronze_df.drop("trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_location", "dropoff_centroid_location", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude")

# based on the official documentation, trips with pickup area null are outside chicago city limits
bronze_df = bronze_df.withColumn("pickup_outside_chicago", col("pickup_community_area").isNull())
bronze_df = bronze_df.withColumn("dropoff_outside_chicago", col("dropoff_community_area").isNull())

# based on the official documentation rows with tip null are cash payments, so we can fill null values in tips with 0
bronze_df = bronze_df.withColumn("tip_mightbe_cash", col("tips").isNull())

# save new dataframe as partitioned parquet file based on month and year
silver_df = (
    bronze_df
    .withColumn("year", year(col("trip_start_timestamp")))
    .withColumn("month", month(col("trip_start_timestamp")))
)

# final check for schema
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

# save the silver dataframe as partitioned parquet file based on month and year
silver_df.write.partitionBy("year", "month").mode("overwrite").parquet(f"s3a://{BUCKET}/silver/taxi_data/")

#save some metrics in a json file
metrics = {
    "pipeline": "chicago_taxi_silver",
    "execution_timestamp": datetime.now(timezone.utc).isoformat(),

    "records_before_cleaning": records_before,
    "records_after_cleaning": records_after,

    "duplicates_removed": duplicates_removed,

    "null_percentage_per_column": null_percentages,

    "total_discrepancies": total_discrepancies
}

# Convert metrics to JSON
metrics_json = json.dumps(metrics, indent=4)

# Save metrics to S3
metrics_path = f"s3a://{BUCKET}/silver/taxi_data_metrics.json"
spark.sparkContext.parallelize([metrics_json]).saveAsTextFile(metrics_path)

print("Data quality metrics uploaded successfully!")

spark.stop()
