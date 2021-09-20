# dask-interop

Integration tests to demonstrate Dask's interoperability with other systems.

Some example checks:

* PySpark Parquet <=> Dask
* Delta Lake => Dask

## Setup

Create the conda environment on your machine with `conda env create -f envs/dask-interop.yml`.

Activate the environment with `conda activate dask-interop`.

Run the test suite with `pytest tests`.

