# Databricks notebook source
# MAGIC %md
# MAGIC # Fertiliser and Pesticides Maps joined to Parcels
# MAGIC rs.sjoin
# MAGIC l/ha
# MAGIC
# MAGIC https://bdcertification.org.uk/wp/wp-content/uploads/2022/04/BDA-Certification-Organic-Production-Standards-2022-.pdf
# MAGIC
# MAGIC
# MAGIC Check wfm organic fert/pest level
# MAGIC

# COMMAND ----------

import os

import numpy as np
import pandas as pd
import rasterio as rio
import rioxarray as rxr
import xarray as xr
from pyspark.sql import functions as F
from pyspark.sql import types as T
from rioxarray.exceptions import NoDataInBounds, OneDimensionalRaster

import elmo_geo
from elmo_geo.utils.misc import dbfs
from elmo_geo.utils.settings import FOLDER_ODS, FOLDER_STG

elmo_geo.register()

# COMMAND ----------

sf_parcels = dbfs(FOLDER_ODS, True) + "/rpa-parcels-2021_03_16.parquet"
f_ferts = FOLDER_STG + "/ukceh-fertilisers-2010_2015/"
f_pests = FOLDER_STG + "/ukceh-pesticides-2012_2017/"
f_wfm_farm = FOLDER_STG + "/wfm-farm-2023_06_09.feather"
f_wfm_field = FOLDER_STG + "/wfm-field-2023_06_09.feather"

f_fert = FOLDER_ODS + "/elmo_geo-fertilisers-2023_10_05.parquet"
f_pest = FOLDER_ODS + "/elmo_geo-pesticides-2023_10_05.parquet"

f_out = FOLDER_ODS + "awest-wfm_fert_pest-2023_10_17.feather"

# COMMAND ----------

# BboxType = Union[list, tuple][[Union(float, int)]*4]  # xmin, ymin, xmax, ymax


def bbox_overlap(bbox0, bbox1):
    x0, y0, x1, y1 = bbox0
    x2, y2, x3, y3 = bbox1
    return x0 <= x2 and y0 <= y2 and x3 <= x1 and y3 <= y1


def equal_res(da0, da1):
    return all(da0.x == da1.x) and all(da0.y == da1.y)


def da_clip_mean(col, da):
    @F.udf(T.ArrayType(T.DoubleType()))
    def _udf(geometry):
        try:
            return da.rio.clip([geometry], all_touched=True).mean(dim=("x", "y")).data.tolist()
        except (OneDimensionalRaster, NoDataInBounds):
            return

    return _udf(col)


def da_clip_sum(col, da):
    @F.udf(T.ArrayType(T.DoubleType()))
    def _udf(geometry):
        try:
            return da.rio.clip([geometry], all_touched=True).sum(dim=("x", "y")).data.tolist()
        except (OneDimensionalRaster, NoDataInBounds):
            return

    return _udf(col)


# TODO: pudf_da_clip_mean


def fert_namer(f):
    return "fertiliser_{0}_prediction\nfertiliser_{0}_uncertainty".format(f.split("_")[1]).split()


def pest_namer(f):
    return "pesticide_{0}_prediction\npesticide_{0}_uncertainty".format(f.split(".")[0]).split()


# COMMAND ----------

# Parcels
sdf_parcels = spark.read.format("geoparquet").load(sf_parcels).repartition(2000)

display(sdf_parcels)

# COMMAND ----------

# Fertilisers
da_fert = xr.concat(
    [
        rxr.open_rasterio(f_ferts + f, masked=True).assign_coords(band=fert_namer(f))
        for f in os.listdir(f_ferts)
    ],
    dim="band",
).fillna(0)


sdf_fert = sdf_parcels.withColumn("_out", da_clip_mean("geometry", da_fert)).select(
    "id_parcel", *[F.expr(f"_out[{i}] AS {name}") for i, name in enumerate(da_fert.band.data)]
)


sdf_fert.write.parquet(dbfs(f_fert, True), mode="overwrite")
display(sdf_fert)

# COMMAND ----------

# Pesticides
da_pest = xr.concat(
    [
        rxr.open_rasterio(f_pests + f, masked=True).assign_coords(band=pest_namer(f))
        for f in os.listdir(f_pests)
    ],
    dim="band",
).fillna(0)


sdf_pest = sdf_parcels.withColumn("_out", da_clip_mean("geometry", da_pest)).select(
    "id_parcel", *[F.expr(f"_out[{i}] AS {name}") for i, name in enumerate(da_pest.band.data)]
)


sdf_pest.write.parquet(dbfs(f_pest, True), mode="overwrite")
display(sdf_pest)

# COMMAND ----------

sdf_pest.write.parquet(dbfs(f_pest, True), mode="overwrite")

# COMMAND ----------

df_wfm_farm = pd.read_feather(f_wfm_farm)
df_wfm_field = pd.read_feather(f_wfm_field)
df_fert = pd.read_parquet(f_fert)
df_pest = pd.read_parquet(f_pest)


df_wfm = pd.DataFrame.merge(
    df_wfm_farm[["id_business", "farm_type"]],
    df_wfm_field[["id_business", "id_parcel", "participation_schemes"]],
    on="id_business",
    how="outer",
)

df = df_wfm.merge(df_fert, on="id_parcel", how="outer").merge(df_pest, on="id_parcel", how="outer")


df.to_feather(f_out)
df

# COMMAND ----------

sdf = spark.read.format("geoparquet").load(sf_parcels)
gdf = sdf.limit(1).toPandas()
g = gdf.at[0, "geometry"]
bbox = (83500.0, 5500.0, 655500.0, 657500.0)
bbox = (300_000, 300_000, 400_000, 400_000)
filepath = f_fert + os.listdir(f_fert)[0]
geometry = g.buffer(100_000)
da = rxr.open_rasterio(filepath, masked=True)


def rasterio_window_open():
    with rio.open(filepath) as src:
        _, _, window = rio.mask.raster_geometry_mask(src, [geometry], crop=True)
        return src.read(window=window, masked=True)


def rasterio_mask_open():
    with rio.open(filepath) as src:
        data, _ = rio.mask.mask(src, [geometry], crop=True, nodata=np.nan)
    return data


def rioxarray_open():
    da = rxr.open_rasterio(filepath, masked=True)
    return da.rio.clip([geometry])


def rioxarray_clip():
    return da.rio.clip([geometry])


def rioxarray_clip2():
    return da.rio.clip_box(*geometry.bounds).rio.clip([geometry])


# %timeit rasterio_mask_open()
# %timeit rasterio_window_open()
# %timeit rioxarray_open()
# %timeit rioxarray_clip()
# %timeit rioxarray_clip2()
