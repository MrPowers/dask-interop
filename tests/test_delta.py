import pytest
import os
import beavis
import pandas as pd
import dask.dataframe as dd
from deltalake import DeltaTable


def test_pyspark_delta_to_dask():
    dt = DeltaTable("resources/delta/1")
    filenames = ["resources/delta/1/" + f for f in dt.files()]

    actual_ddf = dd.read_parquet(filenames, engine="pyarrow")
    df = pd.DataFrame({"name": ["jose", "li", "luisa"], "num": [10, 12, 14]})
    expected_ddf = dd.from_pandas(df, npartitions=1)
    beavis.assert_dd_equality(actual_ddf, expected_ddf, check_index=False)


def test_delta_timetravel():
    dt = DeltaTable("resources/delta/2")
    dt.load_version(0)
    filenames = ["resources/delta/2/" + f for f in dt.files()]

    actual_ddf = dd.read_parquet(filenames, engine="pyarrow")
    print("***")
    print(actual_ddf.compute())
    # df = pd.DataFrame({"name": ["jose", "li", "luisa"], "num": [10, 12, 14]})
    # expected_ddf = dd.from_pandas(df, npartitions=1)
    # beavis.assert_dd_equality(actual_ddf, expected_ddf, check_index=False)
