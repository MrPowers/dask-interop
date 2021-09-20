import pytest
import os
import chispa
import beavis
import dask.dataframe as dd
import dask_deltatable as ddt
import pandas as pd

from .spark import *


def test_pyspark_delta_to_dask():
    # write Delta files with PySpark
    data = [("jose", 10), ("li", 12), ("luisa", 14)]
    df = spark.createDataFrame(data, ["name", "num"])
    df.write.mode("overwrite").format("delta").save("tmp/some-delta-pyspark")

    # verify contents of Delta files with PySpark
    actual_df = spark.read.format("delta").load("tmp/some-delta-pyspark")
    chispa.assert_df_equality(actual_df, df, ignore_row_order=True)

    # verify contents of delta files with Dask
    actual_ddf = ddt.read_delta_table("tmp/some-delta-pyspark", engine="pyarrow")
    df = pd.DataFrame({"name": ["jose", "li", "luisa"], "num": [10, 12, 14]})
    expected_ddf = dd.from_pandas(df, npartitions=1)
    beavis.assert_dd_equality(
        actual_ddf.sort_values(by="num"), expected_ddf, check_index=False
    )
