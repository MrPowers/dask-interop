from pathlib import Path
import shutil

from pyspark.sql import SparkSession

from delta import *

dirpath = Path("resources") / "parquet"
if dirpath.exists() and dirpath.is_dir():
    shutil.rmtree(dirpath)

spark = SparkSession.builder.appName("dask-interop").getOrCreate()

# write Parquet files with PySpark
data = [("jose", 10), ("li", 12), ("luisa", 14)]
df = spark.createDataFrame(data, ["name", "num"])
df.write.mode("overwrite").parquet("resources/parquet/1")

# write partitioned Parquet lake
data = [("a", 1), ("b", 2), ("c", 3), ("a", 4), ("a", 5), ("d", 6)]
df = spark.createDataFrame(data, ["letter", "number"])
df.write.mode("overwrite").partitionBy("letter").parquet("resources/parquet/2")
