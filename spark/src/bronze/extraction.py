import csv
import io
import logging
import os
import time
import boto3
import dotenv
import requests
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


def extract_to_csv(api_url, start_date, end_date, output_file, batch_size, max_retries):
    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)

    offset = 0
    with requests.Session() as session, open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = None

        while True:
            params = {
                "$where": (
                    f"trip_start_timestamp >= '{start_date}' "
                    f"AND trip_start_timestamp < '{end_date}'"
                ),
                "$order": "trip_start_timestamp ASC, trip_id ASC",
                "$limit": batch_size,
                "$offset": offset,
            }

            for attempt in range(max_retries):
                try:
                    response = session.get(api_url, params=params, timeout=(10, 300))
                    response.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:
                        raise
                    wait = 2**attempt
                    logger.warning("Request failed: %s. Retrying in %s seconds...", e, wait)
                    time.sleep(wait)

            reader = csv.DictReader(io.StringIO(response.text))
            rows = list(reader)
            if not rows:
                break

            if writer is None:
                writer = csv.DictWriter(f, fieldnames=reader.fieldnames, extrasaction="ignore")
                writer.writeheader()

            writer.writerows(rows)
            offset += len(rows)
            logger.info("Downloaded %s records so far", f"{offset:,}")

            if len(rows) < batch_size:
                break
            time.sleep(0.5)

    logger.info("Extraction completed: %s records", f"{offset:,}")


def upload_to_bucket(s3_endpoint, access_key, secret_key, bucket, object_key, file_path):
    logger.info("Connecting to object storage at %s", s3_endpoint)
    s3 = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )

    try:
        s3.create_bucket(Bucket=bucket)
        logger.info("Created bucket %s", bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise
        logger.info("Bucket %s already exists", bucket)

    logger.info("Uploading %s bytes to s3://%s/%s", os.path.getsize(file_path), bucket, object_key)
    s3.upload_file(file_path, bucket, object_key)
    logger.info("Upload completed successfully")

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    dotenv.load_dotenv()

    s3_endpoint = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    bucket = os.getenv("BUCKET_NAME")
    object_key = os.getenv("BRONZE_OBJECT_KEY")
    file_path = os.getenv("RAW_DATA_PATH")
    access_key = os.getenv("RUSTFS_USERNAME")
    secret_key = os.getenv("RUSTFS_PASSWORD")

    api_url = os.getenv("API_URL", "https://data.cityofchicago.org/resource/wrvz-psew.csv")
    start_date = os.getenv("START_DATE", "2023-01-01T00:00:00")
    end_date = os.getenv("END_DATE", "2023-01-02T00:00:00")
    batch_size = int(os.getenv("BATCH_SIZE", "2000"))
    max_retries = int(os.getenv("MAX_RETRIES", "5"))

    try:
        required = {
            "BUCKET_NAME": bucket,
            "BRONZE_OBJECT_KEY": object_key,
            "RAW_DATA_PATH": file_path,
            "RUSTFS_USERNAME": access_key,
            "RUSTFS_PASSWORD": secret_key,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError("Missing required environment variables: " + ", ".join(missing))

        extract_to_csv(api_url, start_date, end_date, file_path, batch_size, max_retries)
        upload_to_bucket(s3_endpoint, access_key, secret_key, bucket, object_key, file_path)
    except Exception:
        logger.exception("Bronze extraction/upload failed")
        raise


if __name__ == "__main__":
    main()