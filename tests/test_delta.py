import pytest
import os
import beavis
import dask.dataframe as dd
import pandas as pd
from deltalake import DeltaTable


def test_pyspark_delta_to_dask():
    dt = DeltaTable("resources/delta/1")
    filenames = ["resources/delta/1/" + f for f in dt.files()]

    actual_ddf = dd.read_parquet(filenames, engine="pyarrow")
    df = pd.DataFrame({"name": ["jose", "li", "luisa"], "num": [10, 12, 14]})
    expected_ddf = dd.from_pandas(df, npartitions=1)
    beavis.assert_dd_equality(actual_ddf, expected_ddf, check_index=False)
