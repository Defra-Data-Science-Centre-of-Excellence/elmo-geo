# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Choropleth Plot - National Charater Areas
# MAGIC
# MAGIC **Author:** Obi Thompson Sargoni
# MAGIC
# MAGIC **Date:** 03/04/2024
# MAGIC
# MAGIC This notebook produces a choropleth plot from a processed dataset - a dataset which has values linked to parcel IDs. The chosen numeric variable of the processed dataset is aggregated to National Charater Area polygons, which are used to plot the variable at national scale. 
# MAGIC
# MAGIC Parcel IDs are used to link the chosen datasets with the NCA polygons. Therefore, the input dataset must contain an 'id_parcel' field.
# MAGIC
# MAGIC How to use this notebook:
# MAGIC
# MAGIC 1. Using the widgets, choose the processed dataset to plot, the plot variable.
# MAGIC 2. Enter the variable name and surce into the free text widgets. These are used in the plot.
# MAGIC 3. Run the notebook
# MAGIC
# MAGIC Key processing steps:
# MAGIC 1. The plot variable is aggregated to give a single value per parcel. It is aggregated by summing values for each parcel id.
# MAGIC 2. Then the plot variable is aggregated to National Character Areas (NCAs) by calculating the mean across all parcels within each NCA
# MAGIC
# MAGIC The defaults for this notebook produce a choropleth plot of the proportion of parcels intersected by SHINE geometries (non-designated historic features).

# COMMAND ----------

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
from functools import partial
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DoubleType, FloatType, IntegerType, LongType

from elmo_geo import LOG, register
from elmo_geo.datasets.datasets import datasets, parcels
from elmo_geo.st import sjoin
from elmo_geo.plot.plotting import plot_choropleth_with_head_and_tail_bars

register()

# COMMAND ----------

names = sorted([f"{d.source}/{d.name}/{v.name}" for d in datasets for v in d.versions])
default_name = [n for n in names if "shine" in n][0]
dbutils.widgets.dropdown("dataset", default_name, names)
_, name, version = dbutils.widgets.get("dataset").split("/")
dataset = next(d for d in datasets if d.name == name)

[print(k, v, sep=":\t") for k, v in dataset.__dict__.items()]

parcels_names = sorted([f"{parcels.source}/{parcels.name}/{v.name}" for v in parcels.versions])
default_parcels = [n for n in parcels_names if "adas" in n][0]
dbutils.widgets.dropdown("parcels", default_parcels, parcels_names)
_, pname, pversion = dbutils.widgets.get("parcels").split("/")
path_parcels = next(v.path_read for v in parcels.versions if v.name == pversion)
[print("\n\nname", parcels.name, sep=":\t"), 
 print("version", next(v for v in parcels.versions if v.name == pversion), sep = ":\t"),
 ]

# present fields of the dataset to select which to plot
fields = spark.read.parquet(dataset.path_output.format(version=version)).schema.fields
numeric_variables = [field.name for field in fields if isinstance(field.dataType, (DecimalType, DoubleType, FloatType, IntegerType, LongType))]
dbutils.widgets.dropdown("plot variable", numeric_variables[0], numeric_variables)
value_column = dbutils.widgets.get("plot variable")
print(f"\nDataset variable to plot:\t{value_column}")

dbutils.widgets.text("variable name", "SHINE proportion")
dbutils.widgets.text("variable source", "Historic England SHINE dataset")

variable_name = dbutils.widgets.get("variable name")
variable_source = dbutils.widgets.get("variable source")
print(f"Variable name:\t{variable_name}")
print(f"Variable source:\t{variable_source}")

# COMMAND ----------

# Use national character areas as geometry to aggregate data to
path_nca = "dbfs:/mnt/lab/unrestricted/elm/defra/national_character_areas/2021_03_29/output.parquet"
path_nca_poly = "dbfs:/mnt/lab/unrestricted/elm/defra/national_character_areas/2021_03_29/polygons.parquet"

# COMMAND ----------

spark.read.parquet(dataset.path_output.format(version=version)).display()

# COMMAND ----------

df = (spark.read.parquet(dataset.path_output.format(version=version))
      .groupBy("id_parcel").agg(F.sum(F.col(value_column)).alias(value_column))
      ).toPandas()
df.head()

# COMMAND ----------

# join to complete set of parcels
parcels = (spark.read.format("geoparquet").load(path_parcels)
           .select("id_parcel")
           ).toPandas()
df = parcels.set_index("id_parcel").join(df.set_index("id_parcel"), how = "left")
df

# COMMAND ----------

df.dropna()

# COMMAND ----------

df_nca = spark.read.parquet(path_nca).repartition(200).toPandas()
df_nca = df_nca.sort_values("proportion", ascending=False).drop_duplicates(subset=["id_parcel"]).drop(columns=[
#  "partition",
    "proportion"
])
df_nca

# COMMAND ----------

# check parcel counts in each NCA
df.join(df_nca.set_index("id_parcel"), how="inner").groupby("nca_name").count()[value_column].plot.hist(figsize=(20,6), bins=100)

# COMMAND ----------

# smallest NCAs by parcel count
df.join(df_nca.set_index("id_parcel"), how="inner").groupby("nca_name").count().sort_values(by=value_column, ascending=True).head(20)

# COMMAND ----------

# mean by NCA

# all parcels, with value set to zero where the parcel doesn't appear in the input dataset
df_all = df[[value_column]].fillna(0).join(df_nca.set_index("id_parcel"), how="inner").groupby("nca_name").mean()

# with only parcels that do appear in the input dataset
df_feature = df.dropna(subset=[value_column]).join(df_nca.set_index("id_parcel"), how="inner").groupby("nca_name").mean()
df

# COMMAND ----------

polygons = (spark.read.parquet(path_nca_poly)
            .withColumn("geometry", F.expr("ST_GeomFromWKB(geometry)"))
            .groupby("nca_name")
            .agg(F.expr("ST_Union_Aggr(geometry) as geometry"))
            ).toPandas()
polygons = gpd.GeoDataFrame(polygons, crs = "epsg:27700").loc[:, ["nca_name", "geometry"]].set_index("nca_name")
polygons_feature = polygons.join(df_feature).reset_index().sort_values(by=value_column, ascending=False).dropna()
polygons_all = polygons.join(df_all).reset_index().sort_values(by=value_column, ascending=False).dropna()
polygons_all

# COMMAND ----------

plot_title = f"{variable_name} for land parcels in England by National Character Area - All parcels"
f, axes = plot_choropleth_with_head_and_tail_bars(polygons_all, value_column, variable_name, variable_source, plot_title)
f.show()

# COMMAND ----------

plot_title = f"{variable_name} for land parcels in England by National Character Area - Feature parcels"
f, axes = plot_choropleth_with_head_and_tail_bars(polygons_feature, value_column, variable_name, variable_source, plot_title)
f.show()
