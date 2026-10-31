FROM eclipse-temurin:17-jre

WORKDIR /app

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV SPARK_LOCAL_IP=127.0.0.1

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip3 install --break-system-packages -r requirements.txt

# Warm Ivy cache so S3A jars are present at runtime
RUN python3 - <<'PY'
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("prefetch-jars")
    .master("local[1]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config(
        "spark.jars.packages",
        ",".join(
            [
                "org.apache.hadoop:hadoop-aws:3.5.0",
                "software.amazon.awssdk:bundle:2.35.4",
                "software.amazon.s3.analyticsaccelerator:analyticsaccelerator-s3:1.3.1",
            ]
        ),
    )
    .getOrCreate()
)
spark.stop()
PY

COPY spark/src/ ./src/
COPY spark/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python3", "src/load_raw_data.py"]

