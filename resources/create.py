from pyspark.sql import SparkSession

from delta import *

builder = (
    SparkSession.builder.appName("dask-interop")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# write Parquet files with PySpark
data = [("jose", 10), ("li", 12), ("luisa", 14)]
df = spark.createDataFrame(data, ["name", "num"])
df.write.mode("overwrite").parquet("resources/parquet/1")

# write Delta files with PySpark
data = [("jose", 10), ("li", 12), ("luisa", 14)]
df = spark.createDataFrame(data, ["name", "num"])
df.write.mode("overwrite").format("delta").save("resources/delta/1")

