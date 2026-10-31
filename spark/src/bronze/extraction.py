import logging
import os
import boto3
import dotenv
from botocore.exceptions import BotoCoreError, ClientError

logging.basicConfig(level=logging.INFO)
dotenv.load_dotenv()

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
BUCKET = os.getenv("BUCKET_NAME")
OBJECT_KEY = os.getenv("BRONZE_OBJECT_KEY")
FILE_PATH = os.getenv("RAW_DATA_PATH")

if not BUCKET:
    raise SystemExit("BUCKET_NAME is not set")
if not OBJECT_KEY:
    raise SystemExit("BRONZE_OBJECT_KEY is not set")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=os.getenv("RUSTFS_USERNAME"),
    aws_secret_access_key=os.getenv("RUSTFS_PASSWORD"),
    region_name="us-east-1",
)

existing_buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
if BUCKET not in existing_buckets:
    s3.create_bucket(Bucket=BUCKET)

try:
    s3.upload_file(FILE_PATH, BUCKET, OBJECT_KEY)
    logging.info("Upload successful!")
except (ClientError, BotoCoreError, OSError) as e:
    logging.error("Upload failed: %s", e)
    raise
