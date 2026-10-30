import os
import csv
import io
import time
import requests

URL = "https://data.cityofchicago.org/resource/wrvz-psew.csv"

START_DATE = "2020-02-14T16:15:00"
END_DATE = "2024-01-01T00:00:00"

OUTPUT_FILE = "data/raw_data/chicago_taxi_trips_2020_2023.csv"

BATCH_SIZE = 20_000
MAX_RETRIES = 5

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

where = (
    f"trip_start_timestamp >= '{START_DATE}' "
    f"AND trip_start_timestamp < '{END_DATE}'"
)

offset = 0
total = 0

with requests.Session() as session:
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = None

        while True:
            params = {
                "$where": where,
                "$order": "trip_start_timestamp ASC",
                "$limit": BATCH_SIZE,
                "$offset": offset,
            }

            # Try downloading the batch up to 5 times
            for attempt in range(MAX_RETRIES):
                try:
                    response = session.get(
                        URL, params=params, timeout=120
                    )
                    response.raise_for_status()

                    reader = csv.DictReader(io.StringIO(response.text))
                    rows = list(reader)
                    break

                except requests.exceptions.RequestException as e:
                    if attempt == MAX_RETRIES - 1:
                        raise

                    wait = 2 ** attempt
                    print(f"Request failed: {e}")
                    print(f"Retrying in {wait} seconds...")
                    time.sleep(wait)

            if not rows:
                break

            if writer is None:
                writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
                writer.writeheader()

            writer.writerows(rows)
            f.flush()

            total += len(rows)
            offset += len(rows)

            print(f"Downloaded {total:,} rows")

            if len(rows) < BATCH_SIZE:
                break

            time.sleep(0.5)

print(f"Finished! Total rows: {total:,}")
