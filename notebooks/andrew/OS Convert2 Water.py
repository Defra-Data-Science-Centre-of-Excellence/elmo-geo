# Databricks notebook source
import os
from glob import glob
from math import ceil

import geopandas as gpd
import pandas as pd
import pyogrio
from geopandas.io.arrow import _geopandas_to_arrow
from pyarrow.dataset import write_dataset
from pyspark.sql import functions as F
from pyspark.sql import types as T


def batch_file(f_in, f_out, batch):
    n = pyogrio.read_info(f_in)["features"]
    return pd.DataFrame(
        {
            "f_in": f_in,
            "f_out": f_out,
            "i": range(ceil(n / batch)),
            "batch": batch,
            "status": False,
        }
    )


def batch_folder(path_in, ext, path_out, batch):
    def out(f_in):
        product = f_in.replace(path_in, "").replace(ext, "")
        f_out = path_out + product + ".parquet/"
        return f_out

    return pd.concat(batch_file(f, out(f), batch) for f in glob(path_in + "*" + ext))


def convert():
    @F.udf(T.BooleanType())
    def udf_convert(f_in, f_out, i, batch, status):
        if status:
            return status
        df = pyogrio.read_dataframe(f_in, skip_features=i * batch, max_features=batch)
        table = _geopandas_to_arrow(df)
        write_dataset(table, f_out, format="parquet")
        return True

    return udf_convert("f_in", "f_out", "i", "batch", "status")


def batch_status(path_out):
    f_status = path_out + "status.parquet"
    sf_status = f_status.replace("/dbfs/", "dbfs:/")
    if not os.path.exists(f_status):
        sdf = batch_folder(path_in, ext, path_out, batch)
        sdf.toPandas().to_parquet(f_status)
    return f_status


def batch_convert_folder(path_in, ext, path_out, batch=10_000, engine="pyogrio"):
    f_status = batch_status(path_out)
    df = (
        pd.read_parquet(f_status)
        .pipe(spark.createDataFrame)
        .repartition(sdf.count())
        .withColumn("status", convert())
        .toPandas()
    )
    sdf.to_parquet(f_status)
    display(sdf)


# COMMAND ----------

import os
from glob import glob
from math import ceil

import pyogrio
from geopandas.io.arrow import _geopandas_to_arrow
from pyarrow.dataset import write_dataset

# COMMAND ----------

f_in = "/dbfs/mnt/lab/unrestricted/elm_data/os/mmtopo_gpkg/wtr_fts_water.zip"
batch = 10_000

n = pyogrio.read_info(f)["features"]
N = ceil(n / batch)
for i in range(N):
    print(f"\r{i}/{N}", end="")
    f_out = f_in.replace("mmtopo_gpkg", "mmtopo_pq2").replace(".zip", ".parquet/")
    df = pyogrio.read_dataframe(f_in, skip_features=i * batch, max_features=batch)
    table = _geopandas_to_arrow(df)
    write_dataset(table, f_out, format="parquet")


# COMMAND ----------

# MAGIC %md # Waterbodies Join

# COMMAND ----------

# MAGIC %pip install -q git+https://github.com/aw-west-defra/cdap_geo.git

# COMMAND ----------

from cdap_geo.sedona import F, st_fromwkb, st_join, st_register

st_register()


sf_parcel = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet/"
sf_water = "dbfs:/mnt/lab/unrestricted/elm_data/os/mmtopo_pq2/wtr_fts_water.parquet/"

sf_geom = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/waterbody_geometries.parquet/"
sf_out = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/waterbody.parquet/"


sdf_geom = st_join(
    spark.read.parquet(sf_parcel).filter("geometry IS NOT null"),
    spark.read.parquet(sf_water)
    .select(
        "theme",
        "description",
        "watertype",
        "capturespecification",
        st_fromwkb("geometry"),
    )
    .filter("geometry IS NOT null"),
    lsuffix="_parcel",
    rsuffix="_water",
    distance=12,
)

sdf_geom.write.parquet(sf_geom)
display(sdf_geom)


buf = (
    lambda x: f"ST_Area(ST_Intersection(ST_MakeValid(ST_Buffer(geometry_water, {x})), geometry_parcel))/10000 AS ha_buf{x}"
)

sdf = (
    spark.read.parquet(sf_geom)
    .groupby("id_business", "id_parcel")
    .agg(
        F.expr("FIRST(geometry_parcel) AS geometry_parcel"),
        F.expr("ST_Union_Aggr(geometry_water) AS geometry_water"),
    )
    .select(
        "id_business",
        "id_parcel",
        F.expr(buf(4)),
        F.expr(buf(6)),
        F.expr(buf(8)),
        F.expr(buf(10)),
        F.expr(buf(12)),
    )
)

sdf.write.parquet(sf_out)
display(sdf)
