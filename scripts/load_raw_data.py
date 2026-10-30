
import os
import csv
import io
import time
import requests

URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"

START_DATE = "2020-01-01T00:00:00"
END_DATE = "2021-01-01T00:00:00"

OUTPUT_FILE = "data/raw_data/chicago_taxi_trips_2020_2023.csv"

BATCH_SIZE = 2000
MAX_RETRIES = 5

# MANUAL CHECKPOINT
# None for the first run.
# After a crash, paste the last printed timestamp here.
LAST_TIMESTAMP = "2021-05-18T18:45:00.000" # make None for first run

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

file_exists = (
    os.path.exists(OUTPUT_FILE)
    and os.path.getsize(OUTPUT_FILE) > 0
)

with requests.Session() as session:
    with open(
        OUTPUT_FILE,
        "a" if file_exists else "w",
        newline="",
        encoding="utf-8"
    ) as f:

        while True:

            where = (
                f"trip_start_timestamp >= '{START_DATE}' "
                f"AND trip_start_timestamp < '{END_DATE}'"
            )

            if LAST_TIMESTAMP is not None:
                where += (
                    f" AND trip_start_timestamp > '{LAST_TIMESTAMP}'"
                )

            params = {
                "$where": where,
                "$order": "trip_start_timestamp ASC",
                "$limit": BATCH_SIZE
            }

            rows = []

            for attempt in range(MAX_RETRIES):
                try:
                    response = session.get(
                        URL,
                        params=params,
                        timeout=(10, 300)
                    )
                    response.raise_for_status()

                    reader = csv.DictReader(io.StringIO(response.text))
                    rows = list(reader)
                    fieldnames = reader.fieldnames
                    break

                except requests.exceptions.RequestException as e:
                    if attempt == MAX_RETRIES - 1:
                        raise

                    wait = 2 ** attempt
                    print(f"Request failed: {e}")
                    print(f"Retrying in {wait} seconds...")
                    time.sleep(wait)

            if not rows:
                print("No more records. Extraction completed!")
                break

            writer = csv.DictWriter(f, fieldnames=fieldnames)

            if f.tell() == 0:
                writer.writeheader()

            writer.writerows(rows)
            f.flush()

            # Update checkpoint
            LAST_TIMESTAMP = rows[-1]["trip_start_timestamp"]

            print(f"Downloaded {len(rows):,} records")
            print("COPY THIS VALUE IF THE SCRIPT CRASHES:")
            print(f"LAST_TIMESTAMP = '{LAST_TIMESTAMP}'")
            print("-" * 50)

            if len(rows) < BATCH_SIZE:
                break

            time.sleep(0.5)

print("Finished!")
