# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %pip install -Uq seaborn mapclassify matplotlib

# COMMAND ----------

import geopandas as gpd
import mapclassify
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter, FuncFormatter
import numpy as np

import os
import geopandas as gpd
from pyspark.sql import functions as F

from elmo_geo import LOG, register
from elmo_geo.st import sjoin
from elmo_geo.plot.plotting import plot_choropleth_with_head_and_tail_bars

register()

# COMMAND ----------

year = 2023
path = f"/mnt/lab/unrestricted/elm/elmo/baresoil/output-{year}.parquet"
path_nca = "dbfs:/mnt/lab/unrestricted/elm/elmo/national_character_areas/output.parquet"
path_nca_poly = "/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_character_areas/format_SHP_national_character_areas/LATEST_national_character_areas/National_Character_Areas___Natural_England.shp"


# COMMAND ----------

df = spark.read.parquet(path).toPandas()
mean = df.bare_soil_percent.mean()
df

# COMMAND ----------

df_nca = spark.read.parquet(path_nca).repartition(200).toPandas()
df_nca = df_nca.sort_values("proportion", ascending=False).drop_duplicates(subset=["id_parcel"]).drop(columns=[
#  "partition",
    "proportion"
])
df_nca

# COMMAND ----------

# check parcel counts in each NCA
df.join(df_nca, on = "id_parcel", how="inner").drop(columns="tile").groupby("nca_name").count()["bare_soil_percent"].plot.hist(figsize=(20,6), bins=100)

# COMMAND ----------

# smallest NCAs by parcel count
df.join(df_nca, on = "id_parcel", how="inner").drop(columns="tile").groupby("nca_name").count().sort_values(by="bare_soil_percent", ascending=True).head(20)

# COMMAND ----------

# mean parcel bare soil % by NCA
df = df.join(df_nca, on = "id_parcel", how="inner").drop(columns="tile").groupby("nca_name").mean()
df

# COMMAND ----------

# join in the geometries
polygons = gpd.read_file(path_nca_poly).loc[:, ["nca_name", "geometry"]].rename({"NCA_Name": "nca_name"}, axis=1).to_crs(27700).set_index("nca_name")
polygons = polygons.join(df).reset_index().sort_values(by="bare_soil_percent", ascending=False).dropna()
polygons

# COMMAND ----------

variable_name = "Mean bare soil %"
variable_source = "Sentinel-2 L2A imagery, bare soil defined where NDVI<0.25"
plot_title = f"{variable_name} for land parcels in England by National Character Area - Feature parcels"
f = plot_choropleth_with_head_and_tail_bars(polygons, "bare_soil_percent", variable_name, variable_source, plot_title)
f.show()
