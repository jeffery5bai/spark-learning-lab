from pyspark.sql import SparkSession

spark: SparkSession = (
    SparkSession.builder
    .appName("ep01-first-run")
    .master("local[2]")
    .getOrCreate()
)

print(f"Spark version: {spark.version}")
print(f"Master: {spark.sparkContext.master}")
print(f"Application ID: {spark.sparkContext.applicationId}")

numbers = spark.range(1, 6)

print("DataFrame preview:")
numbers.show()

print(f"Row count: {numbers.count()}")

spark.stop()