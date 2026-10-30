from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("VersionCheck")
    .master("local[1]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

print("Spark version:", spark.version)
print(
    "Hadoop version:",
    spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
)

spark.stop()
