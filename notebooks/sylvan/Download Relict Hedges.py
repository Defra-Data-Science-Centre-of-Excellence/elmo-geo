# Databricks notebook source
# MAGIC %pip install contextily rich

# COMMAND ----------

import json

import geopandas as gpd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from shapely import from_wkt

from elmo_geo import register

register()
# from sedona.register import SedonaRegistrator
# SedonaRegistrator.registerAll(spark)

# COMMAND ----------

sf_relict_segments_out = "dbfs:/mnt/lab/unrestricted/elm/elm_se/relict_hedge/elm_se-relict_segments-2023_08.parquet"
sf_woody_boundaries_out = "dbfs:/mnt/lab/unrestricted/elm/elm_se/relict_hedge/elm_se-woody_boundaries-2023_08.parquet"

# COMMAND ----------

sdf_wb = spark.read.parquet(sf_woody_boundaries_out)
sdf_rh = spark.read.parquet(sf_relict_segments_out)

# COMMAND ----------

sdf_wb.select("major_grid", "bng_10km", "geometry_relict_boundary").display()

# COMMAND ----------

sdf_wb = sdf_wb.select("major_grid", "bng_10km", "bng_1km", "id_parcel", F.expr("ST_AsTEXT(geometry_relict_boundary) as geometry_relict_boundary"))

# COMMAND ----------

df_wb = sdf_wb.toPandas()

# COMMAND ----------

df_wb.shape

# COMMAND ----------

gdf_wb = gpd.GeoDataFrame(df_wb, geometry=df_wb.geometry_relict_boundary.map(lambda x: from_wkt(x)), crs="epsg:27700")

# COMMAND ----------

gdf_wb.drop("geometry_relict_boundary", axis=1).to_file("/dbfs/FileStore/relict_parcel_boundaries.geojson")

# COMMAND ----------

sdf_wb.write.mode("overwrite").parquet("dbfs:/FileStore/relict_parcel_boundaries.parquet")

# COMMAND ----------


def download_file(filepath, filestore_path="/dbfs/FileStore", move=True, spark=None) -> str:
    # Get spark Session
    if not spark:
        spark = SparkSession.getActiveSession()
    # Get filename
    filename = filepath[filepath.rfind("/") :]
    # Construct download url
    url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/files/{filename}?o={spark.conf.get('spark.databricks.clusterUsageTags.orgId')}"
    # Return html snippet
    return f"<a href={url} target='_blank'>Download file: {filename}</a>"


# COMMAND ----------

download_file("/dbfs/FileStore/relict_parcel_boundaries.geojson", filestore_path="/dbfs/FileStore", move=False, spark=spark)

# COMMAND ----------

gdf_test = gpd.read_file("/dbfs/FileStore/relict_parcel_boundaries.geojson")
gdf_test.shape

# COMMAND ----------


upload_json = None
with open("/dbfs/FileStore/relict_parcel_boundaries_upload.geojson") as f:
    upload_json = json.load(f)

# COMMAND ----------

orig_json = None
with open("/dbfs/FileStore/relict_parcel_boundaries.geojson") as f:
    orig_json = json.load(f)

# COMMAND ----------

f = open("/dbfs/FileStore/relict_parcel_boundaries_upload.geojson")
lines = f.readlines()

# COMMAND ----------

len(lines), len(orig_json)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exporting relict hedge length

# COMMAND ----------

sdf_wb_len = sdf_wb.select("id_parcel", F.expr("ST_Length(geometry_relict_boundary) as length_relict_boundary"))

# COMMAND ----------

df_wb_len = sdf_wb_len.toPandas()
df_wb_len.to_csv("/dbfs/FileStore/relict_parcel_length.csv", index=False)
download_file("/dbfs/FileStore/relict_parcel_length.csv", filestore_path="/dbfs/FileStore", move=False, spark=spark)

# COMMAND ----------
