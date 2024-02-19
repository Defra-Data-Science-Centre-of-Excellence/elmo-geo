# Databricks notebook source
# MAGIC %md
# MAGIC # GeoParquet Conversion
# MAGIC Convert large geovector files to geoparquet.
# MAGIC `batch_convert_folder(path_in, ext, path_out)`
# MAGIC
# MAGIC ### Distributedly
# MAGIC
# MAGIC
# MAGIC ### Engine
# MAGIC Using pyogrio on zip files seems fastest.
# MAGIC This could be a GeoPackage bias.
# MAGIC > | File       | Extension | Engine  | Execution Time     |
# MAGIC > |------------|-----------|---------|--------------------|
# MAGIC > | small_file | .zip      | fiona   |   5.18 s ±  359 ms |
# MAGIC > | small_file | .gpkg     | fiona   |   5.18 s ±  102 ms |
# MAGIC > | small_file | .zip      | pyogrio |   2.13 s ±  602 ms |
# MAGIC > | small_file | .gpkg     | pyogrio |   1.32 s ±   71 ms |
# MAGIC > | large_file | .zip      | fiona   | 245    s ± 1180 ms |
# MAGIC > | large_file | .gpkg     | fiona   | 260    s ± 1210 ms |
# MAGIC > | large_file | .zip      | pyogrio |  22.8  s ±  943 ms |
# MAGIC > | large_file | .gpkg     | pyogrio |  25.3  s ±  408 ms |
# MAGIC >
# MAGIC > Table of timeits for reading methods
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC DIR=/dbfs/mnt/lab/unrestricted/elm_data/os/mmtopo_gpkg/*.zip
# MAGIC ls -s $DIR

# COMMAND ----------

import os
from glob import glob
from math import ceil

import fiona
import geopandas as gpd
import pandas as pd
import pyogrio
from geopandas.io.arrow import _geopandas_to_arrow
from pyarrow.dataset import write_dataset
from pyspark.sql import functions as F
from pyspark.sql import types as T


def n_fiona(f_in):
    with fiona.open(f_in) as vfile:
        n = len(vfile)
    return n


def n_pyogrio(f_in):
    return pyogrio.read_info(f_in)["features"]


def calc_n(f_in, engine):
    if engine == "pyogrio":
        return n_pyogrio(f_in)
    elif engine == "fiona":
        return n_fiona(f_in)


def batch_file(f_in, f_out, batch, engine):
    n = calc_n(f_in, engine)
    batch_lower = list(range(0, n, batch))
    batch_upper = *batch_lower[1:], n
    return pd.DataFrame(
        {
            "f_in": f_in,
            "f_out": f_out,
            "i": range(ceil(n / batch)),
            "batch": batch,
            "mn": batch_lower,
            "mx": batch_upper,
            "status": False,
        }
    ).pipe(spark.createDataFrame)


def batch_folder(path_in, ext, path_out, batch, engine):
    for i, f_in in enumerate(glob(path_in + "*" + ext)[:4]):
        product = f_in.replace(path_in, "").replace(ext, "")
        f_out = path_out + product + ".parquet/"
        _sdf = batch_file(f_in, f_out, batch, engine)
        sdf = sdf.union(_sdf) if i else _sdf
    return sdf


def to_parquets(df, f_out):
    table = _geopandas_to_arrow(df)
    write_dataset(table, f_out, format="parquet")


def convert_fiona(f_in, f_out, mn, mx):
    with fiona.open(f_in) as vfile:
        fgen = vfile.filter(mn, mx)
        df = gpd.GeoDataFrame.from_features(fgen)
        to_parquets(df, f_out)


def convert_pyogrio(f_in, f_out, mn, mx):
    batch = mx - mn
    df = pyogrio.read_dataframe(f_in, skip_features=mn, max_features=batch)
    to_parquets(df, f_out)


def convert(engine):
    if engine == "pyogrio":
        return convert_pyogrio(f_in, f_out, mn, mx)
    elif engine == "fiona":
        return convert_fiona(f_in, f_out, mn, mx)

    @F.udf(T.BooleanType())
    def udf_convert(f_in, f_out, mn, mx, status):
        if status:
            return status
        converter(f_in, f_out, mn, mx)
        return True

    return udf_convert("f_in", "f_out", "mn", "mx", "status")


def batch_convert_folder(path_in, ext, path_out, batch=10_000, engine="pyogrio"):
    # Make Status DataFrame
    f_status = path_out + "status.parquet"
    sf_status = f_status.replace("/dbfs/", "dbfs:/")
    if not os.path.exists(f_status):
        sdf = batch_folder(path_in, ext, path_out, batch)
        sdf.toPandas().to_parquet(f_status)
    sdf = spark.read.parquet(sf_status)
    display(sdf)
    # Conversion
    sdf = sdf.repartition(sdf.count()).withColumn("status", convert(engine))
    sdf.toPandas().to_parquet(f_status)
    display(sdf)


# COMMAND ----------

path_in, ext = "/dbfs/mnt/lab/unrestricted/elm_data/os/mmtopo_gpkg/", ".zip"
path_out = "/dbfs/mnt/lab/unrestricted/elm_data/os/mmtopo_pq/"

batch_convert_folder(path_in, ext, path_out)
