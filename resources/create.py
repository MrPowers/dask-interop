from pathlib import Path
import shutil

from pyspark.sql import SparkSession

from delta import *

dirpath = Path("resources") / "parquet"
if dirpath.exists() and dirpath.is_dir():
    shutil.rmtree(dirpath)

dirpath = Path("resources") / "delta"
if dirpath.exists() and dirpath.is_dir():
    shutil.rmtree(dirpath)

builder = (
    SparkSession.builder.appName("dask-interop")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.databricks.delta.schema.autoMerge.enabled", True)
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# write Parquet files with PySpark
data = [("jose", 10), ("li", 12), ("luisa", 14)]
df = spark.createDataFrame(data, ["name", "num"])
df.write.mode("overwrite").parquet("resources/parquet/1")

# write Delta files with PySpark

# 1
data = [("jose", 10), ("li", 12), ("luisa", 14)]
df = spark.createDataFrame(data, ["name", "num"])
df.write.format("delta").save("resources/delta/1")

# 2
data = [("a", 1), ("b", 2), ("c", 3)]
df = spark.createDataFrame(data, ["letter", "number"])
df.write.format("delta").save("resources/delta/2")

data = [("d", 4), ("e", 5), ("f", 6)]
df = spark.createDataFrame(data, ["letter", "number"])
df.write.mode("append").format("delta").save("resources/delta/2")

# 3
data = [("a", 1), ("b", 2), ("c", 3)]
df = spark.createDataFrame(data, ["letter", "number"])
df.write.format("delta").save("resources/delta/3")

data = [("d", 4, "red"), ("e", 5, "blue"), ("f", 6, "green")]
df = spark.createDataFrame(data, ["letter", "number", "color"])
df.write.mode("append").format("delta").save("resources/delta/3")

df = spark.read.format("delta").option("mergeSchema", "true").load("resources/delta/3")
df.show()
