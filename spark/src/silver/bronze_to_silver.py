import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, year, month

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
source_schema = "trip_id STRING, trip_start_timestamp TIMESTAMP, trip_end_timestamp TIMESTAMP, trip_seconds INT, trip_miles DOUBLE, pickup_census_tract STRING, dropoff_census_tract STRING, pickup_community_area INT, dropoff_community_area INT, fare DOUBLE, tips DOUBLE, tolls DOUBLE, extras DOUBLE, trip_total DOUBLE, payment_type STRING, company STRING, pickup_latitude DOUBLE, pickup_longitude DOUBLE, dropoff_latitude DOUBLE, dropoff_longitude DOUBLE"
bronze_df = spark.read.csv(f"s3a://{BUCKET}/{OBJECT_KEY}", header=True, schema=source_schema)

# drop duplicates
bronze_df = bronze_df.dropDuplicates()

#  drop columns that are mostly null
# how many null values are in each column
null_counts = bronze_df.select([sum(bronze_df[col].isNull().cast("int")).alias(col) for col in bronze_df.columns])
null_counts.show()

#percetange of null values in each column
total_rows = bronze_df.count()
null_percentage = null_counts.select([
    (col(column_name) / total_rows * 100).alias(column_name)
    for column_name in null_counts.columns
])

# drop columns with percentage of null values greater than 50%
columns_to_drop = [column_name for column_name in null_percentage.columns if null_percentage.select(column_name).collect()[0][0] > 50]
bronze_df = bronze_df.drop(*columns_to_drop)

# validate that all numbers are positive
bronze_df = bronze_df.filter(
    (col("trip_miles") >= 0) &
    (col("trip_seconds") >= 0) &
    (col("fare") >= 0) &
    (col("tips") >= 0) &
    (col("tolls") >= 0) &
    (col("extras") >= 0) &
    (col("trip_total") >= 0)
)

# make sure trip_total is equal to fare + tips + tolls + extras
# recalculate trip_total
bronze_df = bronze_df.withColumn(
    "calculated_total",
    col("fare") + col("tips") + col("tolls") + col("extras")
)

# drop unnecessary columns
bronze_df = bronze_df.drop("trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_location", "dropoff_centroid_location", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude")

# based on the official documentation, trips with pickup area null are outside chicago city limits
bronze_df = bronze_df.withColumn("pickup_outside_chicago", col("pickup_community_area").isNull())
bronze_df = bronze_df.withColumn("dropoff_outside_chicago", col("dropoff_community_area").isNull())

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
    col("year").cast("int"),
    col("month").cast("int")
)


silver_df.write.partitionBy("year", "month").mode("overwrite").parquet(f"s3a://{BUCKET}/silver/taxi_data/")

spark.stop()
