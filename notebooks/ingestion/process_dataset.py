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
from functools import partial

import geopandas as gpd
import pandas as pd
from pyspark.sql import functions as F

from elmo_geo import LOG, register
from elmo_geo.datasets.datasets import datasets, parcels
from elmo_geo.io import download_link
from elmo_geo.io.preprocessing import make_geometry_valid
from elmo_geo.st import sjoin
from elmo_geo.utils.misc import dbfs

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

# inspect raw data
if os.path.splitext(path_read)[1] == ".parquet":
    spark.read.parquet(dbfs(path_read, True)).display()
else:
    gpd_read = partial(gpd.read_file, engine="pyogrio")
    print(gpd_read(path_read, rows=8))

# COMMAND ----------

# pre-process data
if os.path.splitext(path_read)[1] == ".parquet":
    # load file with spark, filter and rename columns
    mapping = dataset.rename_cols | {c: c for c in dataset.keep_cols if c not in dataset.rename_cols.keys()}
    sdf = spark.read.parquet(dbfs(path_read, True)).select([F.col(c).alias(mapping.get(c, c)) for c in dataset.keep_cols])

else:
    gpd_read = partial(gpd.read_file, engine="pyogrio")

    dissolveby = list(set(dataset.keep_cols) - {"geometry"}) or None

    df = (
        gpd_read(dbfs(path_read, False))
        .dissolve(by=dissolveby)  # union rows where we don't care about the differences in category
        .explode(index_parts=False)  # explode multi-geometries to enable better pararallelisation
        .set_crs("epsg:27700")  # assume CRS
        # .pipe(transform_crs, target_epsg=27700)
        .filter(dataset.keep_cols, axis="columns")
        .rename(columns=dataset.rename_cols)
        .pipe(make_geometry_valid)
        .to_wkb()
    )
    sdf = spark.createDataFrame(df)
    df = None

LOG.info(f"Dataset has {sdf.count():,.0f} rows")
LOG.info(f"Dataset has the following columns: {list(sdf.columns)}")
(sdf.repartition(n_partitions).write.format("parquet").save(dataset.path_polygons.format(version=version), mode="overwrite"))
LOG.info(f"Saved preprocessed dataset to {dataset.path_polygons.format(version=version)}")
sdf.display()

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


df = pd.read_parquet("/dbfs/" + dataset.path_output.format(version=version))
df

# COMMAND ----------

df["id_parcel"].nunique()
