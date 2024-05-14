# Databricks notebook source
# MAGIC %md
# MAGIC # Processing vector data and calculating intersections with land parcels
# MAGIC This notebook is used to clean up vector datasets, and to join them with the land parcels
# MAGIC dataset to get the proportion of the land parcel intersecting with each feature

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import os
import geopandas as gpd
from functools import partial
from pyspark.sql import functions as F

from elmo_geo import LOG, register
from elmo_geo.datasets.datasets import datasets, parcels
from elmo_geo.io import download_link
from elmo_geo.io.preprocessing import geometry_to_wkb, make_geometry_valid, transform_crs
from elmo_geo.st import sjoin

register()

# COMMAND ----------

names = sorted([f"{d.source}/{d.name}/{v.name}" for d in datasets for v in d.versions])
dbutils.widgets.dropdown("dataset", names[-1], names)
_, name, version = dbutils.widgets.get("dataset").split("/")
dataset = next(d for d in datasets if d.name == name)
[print(k, v, sep=":\t") for k, v in dataset.__dict__.items()]

parcels_names = sorted([f"{parcels.source}/{parcels.name}/{v.name}" for v in parcels.versions])
dbutils.widgets.dropdown("parcels", parcels_names[-1], parcels_names)
_, pname, pversion = dbutils.widgets.get("parcels").split("/")
[
    print("\n\nname", parcels.name, sep=":\t"),
    print("version", next(v for v in parcels.versions if v.name == pversion), sep=":\t"),
]

target_epsg = 27700
n_partitions = 200
simplify_tolerence: float = 0.5  # metres
max_vertices: int = 256  # per polygon (row)
path_read = next(v.path_read for v in dataset.versions if v.name == version)
path_parcels = next(v.path_read for v in parcels.versions if v.name == pversion)

# COMMAND ----------

# take a look at the raw data
if os.path.splitext(path_read)[1] == ".parquet":
    gpd_read = gpd.read_parquet
    gpd_read(path_read).head(8)
else:
    gpd_read = partial(gpd.read_file, engine="pyogrio")
    gpd_read(path_read, rows=8)

# COMMAND ----------

# process the dataset
df = (
    gpd_read(path_read)
    .explode(index_parts=False)
    .pipe(transform_crs, target_epsg=27700)
    .filter(dataset.keep_cols, axis="columns")
    .rename(columns=dataset.rename_cols)
    .pipe(make_geometry_valid)
    .pipe(geometry_to_wkb)
)

LOG.info(f"Dataset has {df.size:,.0f} rows")
LOG.info(f"Dataset has the following columns: {df.columns.tolist()}")
(spark.createDataFrame(df).repartition(n_partitions).write.format("parquet").save(dataset.path_polygons.format(version=version), mode="overwrite"))
LOG.info(f"Saved preprocessed dataset to {dataset.path_polygons.format(version=version)}")

# COMMAND ----------

# take a look at the processed data
df = spark.read.parquet(dataset.path_polygons.format(version=version))
df.display()

# COMMAND ----------

# process the parcels dataset to ensure validity, simplify the vertices to a tolerence,
# and subdivide large geometries
df_parcels = (
    spark.read.format("geoparquet")
    .load(path_parcels)
    .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
    .withColumn("geometry", F.expr(f"ST_SimplifyPreserveTopology(geometry, {simplify_tolerence})"))
    .withColumn("geometry", F.expr("ST_Force_2D(geometry)"))
    .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
    .select("id_parcel", "geometry")
)
df_parcels.display()

# COMMAND ----------

# process the feature dataset to ensure validity, simplify the vertices to a tolerence,
# and subdivide large geometries
df_feature = (
    spark.read.parquet(dataset.path_polygons.format(version=version))
    .withColumn("geometry", F.expr("ST_GeomFromWKB(hex(geometry))"))
    .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
    .withColumn("geometry", F.expr(f"ST_SimplifyPreserveTopology(geometry, {simplify_tolerence})"))
    .withColumn("geometry", F.expr("ST_Force_2D(geometry)"))
    .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
    .withColumn("geometry", F.expr(f"ST_SubdivideExplode(geometry, {max_vertices})"))
)
df_feature.display()

# COMMAND ----------

# join the two datasets and calculate the proportion of the parcel that intersects
df = (
    sjoin(df_parcels, df_feature)
    .withColumn("geometry_intersection", F.expr("ST_Intersection(geometry_left, geometry_right)"))
    .withColumn("area_left", F.expr("ST_Area(geometry_left)"))
    .withColumn("area_intersection", F.expr("ST_Area(geometry_intersection)"))
    .withColumn("proportion", F.col("area_intersection") / F.col("area_left"))
    .drop("area_left", "area_intersection", "geometry_left", "geometry_right", "geometry_intersection")
)
# group up the result and sum the proportions in case multiple polygons with
# the same attributes intersect with the parcel
df = (
    df.groupBy(*[col for col in df.columns if col != "proportion"])
    .sum("proportion")
    .withColumn("proportion", F.round("sum(proportion)", 6))
    .where("proportion > 0")
    .drop("sum(proportion)")
)

# intersect the two datasets
(df.write.format("parquet").save(dataset.path_output.format(version=version), mode="overwrite"))

# COMMAND ----------

# show results
result = spark.read.parquet(dataset.path_output.format(version=version))
count = result.count()
LOG.info(f"Rows: {count:,.0f}")
# check proportion is never > 1 - if it is might mean duplicate features int he dataset
proportion_over_1 = (result.toPandas().proportion > 1.0).sum()
if proportion_over_1:
    LOG.info(f"{proportion_over_1:,.0f} parcels have a feature overlapping by a proportion > 1 ({proportion_over_1/count:%})")
result.display()

# COMMAND ----------

result.toPandas().proportion.describe()

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

# check for any issues causing proportion > 1
pandas_df.sort_values("proportion", ascending=False).head(8)

# COMMAND ----------

import pandas as pd

df = pd.read_parquet("/dbfs/" + dataset.path_output.format(version=version))
df

# COMMAND ----------

df["id_parcel"].nunique()
