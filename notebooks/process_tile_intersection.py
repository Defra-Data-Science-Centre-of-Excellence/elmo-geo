# Databricks notebook source
# MAGIC %md
# MAGIC # Intersecting tile data with land parcels
# MAGIC This notebook is used to join sentinel tiles dataset with the land parcels
# MAGIC dataset to get the proportion of the land parcel intersecting with each tile.
# MAGIC It will then input the geometries back into the dataset so it can be ready for use
# MAGICV in the calc_bare_soil_perc notebook.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %pip install  beautifulsoup4 lxml

# COMMAND ----------

import geopandas as gpd
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import concat, expr
from sedona.register import SedonaRegistrator

from elmo_geo.datasets import tiles
from elmo_geo.joins import spatial_join
from elmo_geo.log import LOG
from elmo_geo.preprocessing import geometry_to_wkb, make_geometry_valid, transform_crs
from elmo_geo.sentinel import sentinel_tiles

SedonaRegistrator.registerAll(spark)

# COMMAND ----------

names = sorted([f"{v.name}" for v in tiles.versions])
dbutils.widgets.dropdown("version", names[-1], names)
version = dbutils.widgets.get("dataset")
dataset = tiles.copy()
[print(k, v, sep=":\t") for k, v in dataset.__dict__.items()]
path_parcels = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_02_07.parquet"
path_output = f"dbfs:/mnt/lab/unrestricted/elm/sentinel/tiles/{version}/parcels.parquet"
target_epsg = 27700
n_partitions = 200
simplify_tolerence: float = 0.5  # metres
max_vertices: int = 256  # per polygon (row)
path_read = next(v.path_read for v in tiles.versions if v.name == version)
required_tiles = sentinel_tiles.copy()

# COMMAND ----------

# take a look at the raw data
# gpd.read_file(path_read, engine="pyogrio", rows=8)
pdf = pd.read_parquet(path_read)
gdf = gpd.GeoDataFrame(
    pdf, geometry=gpd.GeoSeries.from_wkb(pdf["geometry"], crs=target_epsg), crs=target_epsg
)
gdf

# COMMAND ----------

# process the dataset
df = (
    gdf.explode(index_parts=False)
    .pipe(transform_crs, target_epsg=27700)
    .filter(dataset.keep_cols, axis="columns")
    .rename(columns=dataset.rename_cols)
    .pipe(make_geometry_valid)
    .pipe(geometry_to_wkb)
)

LOG.info(f"Dataset has {df.size:,.0f} rows")
LOG.info(f"Dataset has the following columns: {df.columns.tolist()}")
(
    spark.createDataFrame(df)
    .repartition(n_partitions)
    .write.format("parquet")
    .save(dataset.path_polygons.format(version=version), mode="overwrite")
)
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
    .filter(F.col("tile").isin(required_tiles))
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

result = spark.read.parquet(dataset.path_output.format(version=version))

# COMMAND ----------

# adding geometries back into the dataset
result = result.filter(result.proportion > 0.99).select("tile", "id_parcel")
df = (
    result.join(df_parcels, ["id_parcel"], "left")
    .select("id_parcel", "tile", "geometry")
    .withColumn("geometry", expr("ST_AsBinary(geometry)"))
)
df.write.format("parquet").save(path_output, mode="overwrite")
display(df)

# COMMAND ----------

# show results
count = result.count()
LOG.info(f"Rows: {count:,.0f}")
# check proportion is never > 1 - if it is might mean duplicate features int he dataset
proportion_over_1 = (result.toPandas().proportion > 1.0).sum()
if proportion_over_1:
    LOG.info(
        f"{proportion_over_1:,.0f} parcels overlap tiles "
        f" by a proportion > 1 ({proportion_over_1/count:%})"
    )
result.display()

# COMMAND ----------

pandas_df = result.toPandas()
# check for any issues causing prtoportion > 1
pandas_df.sort_values("proportion", ascending=False).head(5)

# COMMAND ----------

# count number of proprtions less than 1
pandas_df["bins"] = pd.cut(
    pandas_df["proportion"], [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
)
proportion_df = pandas_df.groupby(by="bins").count()
proportion_df

# COMMAND ----------
