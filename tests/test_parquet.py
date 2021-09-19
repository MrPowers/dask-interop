import pytest
import os
import chispa
import beavis
import dask.dataframe as dd
import pandas as pd

from .spark import *


def test_pyspark_parquet_to_dask():
    # write Parquet files with PySpark
    data = [("jose", 10), ("li", 12), ("luisa", 14)]
    df = spark.createDataFrame(data, ["name", "num"])
    df.write.mode("overwrite").parquet("tmp/some-data-pyspark")

    # verify contents of Parquet files with PySpark
    actual_df = spark.read.parquet("tmp/some-data-pyspark")
    chispa.assert_df_equality(actual_df, df, ignore_row_order=True)

    # verify contents of Parquet files with Dask
    actual_ddf = dd.read_parquet("tmp/some-data-pyspark", engine="pyarrow")
    df = pd.DataFrame({"name": ["jose", "li", "luisa"], "num": [10, 12, 14]})
    expected_ddf = dd.from_pandas(df, npartitions=1)
    beavis.assert_dd_equality(actual_ddf, expected_ddf, check_index=False)


def test_dask_parquet_to_pyspark():
    # write Parquet files with Dask
    df = pd.DataFrame({"name": ["jose", "li", "luisa"], "num": [10, 12, 14]})
    ddf = dd.from_pandas(df, npartitions=1)
    ddf.to_parquet("tmp/some-data-dask", write_index=False)

    # verify contents of Parquet files with PySpark
    data = [("jose", 10), ("li", 12), ("luisa", 14)]
    expected_df = spark.createDataFrame(data, ["name", "num"])
    actual_df = spark.read.parquet("tmp/some-data-dask")
    chispa.assert_df_equality(actual_df, expected_df)

