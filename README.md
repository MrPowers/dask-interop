# dask-interop

Integration tests to demonstrate Dask's interoperability with other systems.

Some example checks:

* PySpark Parquet => Dask
* Delta Lake => Dask

## Setup

Create the conda environment on your machine with `conda env create -f envs/dask-interop.yml`.

Activate the environment with `conda activate dask-interop`.

Run the test suite with `pytest tests`.

## Create resources

The [delta](https://github.com/delta-io/delta) and [delta-rs](https://github.com/delta-io/delta-rs) dependencies conflict, so a single integration test can't write a Delta Lake with delta and read it with delta-rs.

This is probably better anyways.  Delta writes are super-slow and it's good for Dask devs to be able to run the reader tests, even if their machines aren't provisioned for Spark.

The Spark written files used by the tests in this project are checked into version control.  Here's how you can recreate them:

* Create conda environment with `conda env create -f envs/dask-interop-resources.yml`.

* Activate the environment with `conda activate dask-interop-resources`.

* Run the script with `python resources/create.py`.

