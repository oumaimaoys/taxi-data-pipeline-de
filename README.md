# Pipeline Chicago Taxi Trips

description

# prerequis



# setup 


run docker compose file and dockerfile that setup python env and download requiremetns

give permission to obejct-storage

docker compose up

update env.

check spark and hadoop versions and update the spark sessions on the files

# Telechargmeent du data brute

run sript load_raw_data.py

create buckets in rustfs

# Spark memory (bronze_to_silver)

The bronze CSV is about **1.8GB**. Running `spark/src/silver/bronze_to_silver.py` in the Spark container can fail on the first `count()` with:

- `java.lang.OutOfMemoryError: Java heap space`
- or a wrapped `Py4JJavaError: <exception str() failed>` (Py4J cannot print the Java error after the JVM is out of memory)

The job is configured for **2GB driver memory** and **2 cores**. Give the Spark container that RAM, and stop other heavy Docker stacks if the host is tight on memory.

```bash
docker compose run --rm spark python3 src/silver/bronze_to_silver.py
```


