import boto3
import os
import dotenv
from botocore.exceptions import ClientError, BotoCoreError
import logging


logging.basicConfig(level=logging.INFO)
dotenv.load_dotenv()

# init the client
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.getenv("RUSTFS_USERNAME"),
    aws_secret_access_key=os.getenv("RUSTFS_PASSWORD"),
    region_name="us-east-1"
)

BUCKET = "chicago-taxi-datalake"

# create bucket if it doesn't exist
if BUCKET not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
    s3.create_bucket(Bucket=BUCKET)

# Local CSV
file_path = "data/raw_data/chicago_taxi_trips_2020_2021.csv" #idk maybe put it somewhere else like env


# Object key
object_key = "bronze/chicago_taxi/trips_2020_2021.csv"

# Upload

try:
    s3.upload_file(file_path, BUCKET, object_key)
    logging.info("Upload successful!")

except (ClientError, BotoCoreError, Exception) as e:
    logging.error(f"Upload failed: {e}")



