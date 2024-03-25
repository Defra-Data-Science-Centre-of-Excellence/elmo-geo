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

target_epsg = 27700
n_partitions = 200
simplify_tolerence: float = 0.5  # metres
max_vertices: int = 256  # per polygon (row)
path_read = next(v.path_read for v in dataset.versions if v.name == version)
path_parcels = next(v.path_read for v in parcels.versions if v.name == pversion)

# COMMAND ----------

# # take a look at the raw data
# if os.path.splitext(path_read)[1] == ".parquet":
#     gpd_read = gpd.read_parquet
#     gpd_read(path_read).head(8)
# else:
#     gpd_read = partial(gpd.read_file, engine = "pyogrio")
#     gpd_read(path_read, rows=8)

# COMMAND ----------

# # process the dataset
# df = (
#     gpd_read(path_read)
#     .explode(index_parts=False)
#     .pipe(transform_crs, target_epsg=27700)
#     .filter(dataset.keep_cols, axis="columns")
#     .rename(columns=dataset.rename_cols)
#     .pipe(make_geometry_valid)
#     .pipe(geometry_to_wkb)
# )

# LOG.info(f"Dataset has {df.size:,.0f} rows")
# LOG.info(f"Dataset has the following columns: {df.columns.tolist()}")
# (spark.createDataFrame(df).repartition(n_partitions).write.format("parquet").save(dataset.path_polygons.format(version=version), mode="overwrite"))
# LOG.info(f"Saved preprocessed dataset to {dataset.path_polygons.format(version=version)}")

# COMMAND ----------

# take a look at the processed data
df = spark.read.parquet(path_read)
df.display()

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
    
)
df_feature.display()

# COMMAND ----------

df_feature.createOrReplaceTempView("right")
df_parcels.createOrReplaceTempView("left")

# COMMAND ----------

sdf = spark.sql(
    """
    select id_parcel, heath_habitat, distance from (
    select id_parcel, heath_habitat, distance, ROW_NUMBER() OVER(PARTITION BY id_parcel, heath_habitat ORDER BY distance ASC) AS rank
    from (
    SELECT left.id_parcel, right.Main_Habit as heath_habitat, ST_Distance(left.geometry, right.geometry) as distance
    FROM left JOIN right
    on ST_Distance(left.geometry, right.geometry) < 5000
    ) t ) t2
    where rank=1
""",
)
sdf.display()

# COMMAND ----------

# from sedona.core.spatialOperator import KNNQuery
# from sedona.core.enums import IndexType
# from sedona.core.enums import GridType
# from sedona.core.spatialOperator import JoinQuery
# from sedona.utils.adapter import Adapter 

# spatialRDD = Adapter.toSpatialRdd(df_parcels, "checkin")

# build_on_spatial_partitioned_rdd = False ## Set to TRUE only if run join query
# spatial_rdd.buildIndex(IndexType.RTREE, build_on_spatial_partitioned_rdd)

# using_index = True
# result = KNNQuery.SpatialKnnQuery(spatial_rdd, point, k, using_index)

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

result.toPandas().groupby("heath_habitat").distance.describe()

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
