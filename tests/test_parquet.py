import pytest
import os
import beavis
import dask.dataframe as dd
import pandas as pd


def test_pyspark_parquet_to_dask_pyarrow():
    actual_ddf = dd.read_parquet("resources/parquet/1", engine="pyarrow")
    df = pd.DataFrame({"name": ["jose", "li", "luisa"], "num": [10, 12, 14]})
    expected_ddf = dd.from_pandas(df, npartitions=1)
    beavis.assert_dd_equality(actual_ddf, expected_ddf, check_index=False)


def test_pyspark_parquet_to_dask_fastparquet():
    actual_ddf = dd.read_parquet(
        os.path.join("resources/parquet/1", "*.parquet"), engine="fastparquet"
    )
    df = pd.DataFrame({"name": ["jose", "li", "luisa"], "num": [10, 12, 14]})
    expected_ddf = dd.from_pandas(df, npartitions=1)
    beavis.assert_dd_equality(actual_ddf, expected_ddf, check_index=False)
