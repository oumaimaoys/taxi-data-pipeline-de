import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

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

df = spark.read.csv(
    f"s3a://{BUCKET}/{OBJECT_KEY}",
    header=True,
    inferSchema=True,
)

df.show(5)

spark.stop()
