# Databricks notebook source
# MAGIC %md
# MAGIC # Processing vector data and calculating minimum distance to land parcels
# MAGIC **Author:** Obi Thompson Sargoni
# MAGIC
# MAGIC **Date:** 08/04/2024
# MAGIC
# MAGIC This notebook produces a parcel level dataset which gives the minimum distance from each parcel to features in the input features datasets, upto a maximum threshold distance.
# MAGIC
# MAGIC How to use this notebook:
# MAGIC
# MAGIC 1. Using the widgets choose the features dataset and the parcels dataset (which version parcels to be used)
# MAGIC 2. Run the notebook
# MAGIC
# MAGIC To do:
# MAGIC * Implement a KNN methodology that calculates the minimum distance of every parcel to a geometry in the features dataset without imposing a distance threshold.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import os
import geopandas as gpd
from functools import partial
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DoubleType, FloatType, IntegerType, LongType

from elmo_geo import LOG, register
from elmo_geo.datasets.datasets import datasets, parcels
from elmo_geo.io import download_link
from elmo_geo.io.preprocessing import geometry_to_wkb, make_geometry_valid, transform_crs
from elmo_geo.st import sjoin 

register()

# COMMAND ----------

default_parcels = "rpa/parcels/2021_11_16_adas"

# COMMAND ----------

names = sorted([f"{d.source}/{d.name}/{v.name}" for d in datasets for v in d.versions])
dbutils.widgets.dropdown("dataset", names[-1], names)
_, name, version = dbutils.widgets.get("dataset").split("/")
dataset = next(d for d in datasets if d.name == name)
[print(k, v, sep=":\t") for k, v in dataset.__dict__.items()]

parcels_names = sorted([f"{parcels.source}/{parcels.name}/{v.name}" for v in parcels.versions])
dbutils.widgets.dropdown("parcels", default_parcels, parcels_names)
_, pname, pversion = dbutils.widgets.get("parcels").split("/")
[print("\n\nname", parcels.name, sep=":\t"), 
 print("version", next(v for v in parcels.versions if v.name == pversion), sep = ":\t"),
 ]

path_read = next(v.path_read for v in dataset.versions if v.name == version)
path_parcels = next(v.path_read for v in parcels.versions if v.name == pversion)

# present fields of the dataset to select which to plot
fields = spark.read.parquet(path_read).schema.fields
non_numeric_variables = [field.name for field in fields if ~isinstance(field.dataType, (DecimalType, DoubleType, FloatType, IntegerType, LongType))]
dbutils.widgets.dropdown("group-by variable", non_numeric_variables[0], non_numeric_variables)
groupby_variable = dbutils.widgets.get("group-by variable")
print(f"\nDataset variable to group distances by:\t{groupby_variable}")

dbutils.widgets.text("group-by alias", "heath_habitat")
groupby_alias = dbutils.widgets.get("group-by alias")

target_epsg = 27700
n_partitions = 200
simplify_tolerence: float = 0.5  # metres
max_vertices: int = 256  # per polygon (row)

DISTANCE_THRESHOLD = 5_000

# COMMAND ----------

# take a look at the processed data
spark.read.parquet(path_read).display()

# COMMAND ----------

# process the parcels dataset to ensure validity, simplify the vertices to a tolerence,
# and subdivide large geometries
df_parcels = (
    spark.read.format("geoparquet").load(path_parcels)
    .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
    .withColumn("geometry", F.expr(f"ST_SimplifyPreserveTopology(geometry, {simplify_tolerence})"))
    .withColumn("geometry", F.expr("ST_Force_2D(geometry)"))
    .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
    .select("id_parcel", "geometry")
    .repartition(1_000)
)
df_parcels.display()

# COMMAND ----------

# process the feature dataset to ensure validity, simplify the vertices to a tolerence,
# and subdivide large geometries
df_feature = (
    spark.read.parquet(path_read)
    .withColumn("geometry", F.expr("ST_GeomFromWKB(hex(geometry))"))
    .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
    .withColumn("geometry", F.expr(f"ST_SimplifyPreserveTopology(geometry, {simplify_tolerence})"))
    .withColumn("geometry", F.expr("ST_Force_2D(geometry)"))
    .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
    .withColumn("geometry", F.expr(f"ST_SubdivideExplode(geometry, {max_vertices})"))
    .repartition(n_partitions)
    
)
df_feature.display()

# COMMAND ----------

df_feature.createOrReplaceTempView("right")
df_parcels.createOrReplaceTempView("left")

# COMMAND ----------

sdf = spark.sql(
    f"""
    select id_parcel, {groupby_alias}, distance from (
    select id_parcel, {groupby_alias}, distance, ROW_NUMBER() OVER(PARTITION BY id_parcel, {groupby_alias} ORDER BY distance ASC) AS rank
    from (
    SELECT left.id_parcel, right.{groupby_variable} as {groupby_alias}, ST_Distance(left.geometry, right.geometry) as distance
    FROM left JOIN right
    on ST_Distance(left.geometry, right.geometry) < {DISTANCE_THRESHOLD}
    ) t ) t2
    where rank=1
""",
)
sdf.display()

# COMMAND ----------

# save the data
(   
    sdf
    .write.format("parquet")
    .save(dataset.path_output.format(version=version), mode="overwrite")
)

# COMMAND ----------

# show results
result = spark.read.parquet(dataset.path_output.format(version=version))
count = result.count()
LOG.info(f"Rows: {count:,.0f}")

# COMMAND ----------

result.toPandas().groupby(groupby_alias).distance.describe()

# COMMAND ----------

# download
pandas_df = result.toPandas()
path_parquet = "/dbfs" + dataset.path_output.format(version=version).replace("output", dataset.name)
path_feather = "/dbfs" + dataset.path_output.format(version=version).replace("output", dataset.name).replace(".parquet", ".feather")
path_csv = "/dbfs" + dataset.path_output.format(version=version).replace("output", dataset.name).replace(".parquet", ".csv")

# convert types
for col, newtype in dataset.output_coltypes.items():
    pandas_df[col] = pandas_df[col].astype(newtype)

# output
pandas_df.to_parquet(path_parquet)
pandas_df.to_feather(path_feather)
pandas_df.to_csv(path_csv, index=False)
displayHTML(download_link(path_parquet))
displayHTML(download_link(path_feather))
displayHTML(download_link(path_csv))

# COMMAND ----------

import pandas as pd
df = pd.read_parquet("/dbfs/"+dataset.path_output.format(version=version))
df

# COMMAND ----------

df["id_parcel"].nunique()
