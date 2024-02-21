# Databricks notebook source
# MAGIC %md
# MAGIC # Processing vector data and calculating intersections with land parcels
# MAGIC This notebook is used to clean up vector datasets, and to join them with the land parcels
# MAGIC dataset to get the proportion of the land parcel intersecting with each feature

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import geopandas as gpd
from pyspark.sql.functions import concat, expr

from elmo_geo import LOG, register
from elmo_geo.datasets.datasets import datasets
from elmo_geo.io.io import download_link
from elmo_geo.io.preprocessing import (
    geometry_to_wkb,
    make_geometry_valid,
    transform_crs,
)
from elmo_geo.st.joins import spatial_join

register()

# COMMAND ----------

names = sorted([f"{d.source}/{d.name}/{v.name}" for d in datasets for v in d.versions])
dbutils.widgets.dropdown("dataset", names[-1], names)
_, name, version = dbutils.widgets.get("dataset").split("/")
dataset = next(d for d in datasets if d.name == name)
[print(k, v, sep=":\t") for k, v in dataset.__dict__.items()]
path_parcels = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_02_07.parquet"
target_epsg = 27700
n_partitions = 200
simplify_tolerence: float = 0.5  # metres
max_vertices: int = 256  # per polygon (row)
path_read = next(v.path_read for v in dataset.versions if v.name == version)

# COMMAND ----------

# take a look at the raw data
gpd.read_file(path_read, engine="pyogrio", rows=8)

# COMMAND ----------

# process the dataset
df = (
    gpd.read_file(path_read, engine="pyogrio")
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
    spark.read.parquet(path_parcels)
    .withColumn("id_parcel", concat("SHEET_ID", "PARCEL_ID"))
    .withColumn("geometry", expr("ST_GeomFromWKB(wkb_geometry)"))
    .withColumn("geometry", expr("ST_MakeValid(geometry)"))
    .withColumn("geometry", expr(f"ST_SimplifyPreserveTopology(geometry, {simplify_tolerence})"))
    .withColumn("geometry", expr("ST_Force_2D(geometry)"))
    .withColumn("geometry", expr("ST_MakeValid(geometry)"))
    .select("id_parcel", "geometry")
)
df_parcels.display()
# COMMAND ----------

# process the feature dataset to ensure validity, simplify the vertices to a tolerence,
# and subdivide large geometries
df_feature = (
    spark.read.parquet(dataset.path_polygons.format(version=version))
    .withColumn("geometry", expr("ST_GeomFromWKB(hex(geometry))"))
    .withColumn("geometry", expr("ST_MakeValid(geometry)"))
    .withColumn("geometry", expr(f"ST_SimplifyPreserveTopology(geometry, {simplify_tolerence})"))
    .withColumn("geometry", expr("ST_Force_2D(geometry)"))
    .withColumn("geometry", expr("ST_MakeValid(geometry)"))
    .withColumn("geometry", expr(f"ST_SubdivideExplode(geometry, {max_vertices})"))
)
df_feature.display()

# COMMAND ----------

# intersect the two datasets
(
    spatial_join(
        df_left=df_parcels,
        df_right=df_feature,
        spark=spark,
        num_partitions=10000,
    )
    .write.format("parquet")
    .save(dataset.path_output.format(version=version), mode="overwrite")
)

# COMMAND ----------

# show results
result = spark.read.parquet(dataset.path_output.format(version=version))
count = result.count()
LOG.info(f"Rows: {count:,.0f}")
# check proportion is never > 1 - if it is might mean duplicate features int he dataset
proportion_over_1 = (result.toPandas().proportion > 1.0).sum()
if proportion_over_1:
    LOG.info(f"{proportion_over_1:,.0f} parcels have a feature " f"overlapping by a proportion > 1 ({proportion_over_1/count:%})")
result.display()

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
displayHTML(download_link(spark, path_parquet))
displayHTML(download_link(spark, path_feather))
displayHTML(download_link(spark, path_csv))

# COMMAND ----------

# check for any issues causing prtoportion > 1
pandas_df.sort_values("proportion", ascending=False).head(8)

# COMMAND ----------
