from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, avg

spark = SparkSession.builder.appName("HW4Preprocess").getOrCreate()

df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)

df = df.withColumn(
    "fare_per_minute",
    col("fare") / (col("trip_seconds") / 60.0)
)

df.createOrReplaceTempView("trips")

result = spark.sql("""
    SELECT
        company,
        ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute
    FROM trips
    GROUP BY company
    ORDER BY avg_fare_per_minute DESC
""")

result.show(truncate=False)

result.write.mode("overwrite").json("processed_data")

spark.stop()
