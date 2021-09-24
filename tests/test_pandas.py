import pytest
import os
import beavis
import dask.dataframe as dd
import pandas as pd
from pathlib import Path
import shutil


def test_pandas1():
    dirpath = Path("tmp") / "pandas" / "1"
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)
    Path("tmp/pandas/1").mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame({"name": ["jose", "li", "luisa"], "num": [10, 12, 14]})
    df.to_parquet("tmp/pandas/1/file.parquet", engine="pyarrow")

    actual_ddf = dd.read_parquet("tmp/pandas/1", engine="pyarrow")
    expected_ddf = dd.from_pandas(df, npartitions=1)
    beavis.assert_dd_equality(actual_ddf, expected_ddf)


def test_pandas2():
    dirpath = Path("tmp") / "pandas" / "2"
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)
    Path("tmp/pandas/2").mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        data={
            "year": [2012, 2020, 2021],
            "month": [1, 12, 2],
            "day": [1, 31, 28],
            "value": [1000, 2000, 3000],
        }
    )
    df.to_parquet(
        "tmp/pandas/2", engine="pyarrow", partition_cols=["year"], index=False
    )

    actual_ddf = dd.read_parquet("tmp/pandas/2", engine="pyarrow").astype("int64")
    expected_ddf = dd.from_pandas(df, npartitions=1)[["month", "day", "value", "year"]]
    beavis.assert_dd_equality(actual_ddf, expected_ddf, check_index=False)
