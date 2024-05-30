# Databricks notebook source
# MAGIC %pip install -q contextily

# COMMAND ----------

import contextily as ctx
import geopandas as gpd
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

import elmo_geo

elmo_geo.register()
# COMMAND ----------

sf_uptake = "dbfs:/mnt/lab/unrestricted/elm/elm_se/boundary_use_uptake.parquet"
# sf_peat = 'dbfs:/mnt/lab/restricted/ELM-Project/data/defra-peaty_soils-2021_03_24.parquet'
f_peat = "/dbfs/mnt/lab/unrestricted/elm_data/defra-peaty_soils-2021_03_24.parquet"
sf_parcel = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet"


# COMMAND ----------

sdf_uptake = spark.read.parquet(sf_uptake)
sdf_parcel = spark.read.parquet(sf_parcel)


pdf = (
    sdf_uptake.filter("elg_ditch AND 0.1<peatland")
    .select("id_parcel")
    .distinct()
    .join(sdf_parcel, on="id_parcel", how="inner")
    .withColumn("geometry", F.expr("ST_AsBinary(geometry)"))
    .toPandas()
)
gdf = gpd.GeoDataFrame(pdf, geometry=gpd.GeoSeries.from_wkb(pdf["geometry"], crs=27700), crs=27700)


gdf

# COMMAND ----------

gdf_peat = gpd.read_parquet(f_peat)


gdf_peat

# COMMAND ----------

fig, ax = plt.subplots(figsize=(18, 32))
ax.axis("off")

gdf_peat.plot(ax=ax, color="tab:brown")  # column='pclassdesc', cmap='copper
gdf.plot(ax=ax, color="black")
ctx.add_basemap(ax=ax, crs=27700)
