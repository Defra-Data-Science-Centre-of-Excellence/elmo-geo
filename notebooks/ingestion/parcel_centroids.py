# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Export parcel centroids
# MAGIC
# MAGIC Exports parcel centroids.
# MAGIC
# MAGIC Output schema: `id_parcel:str, sindex:str, centroid:wkt`

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import os
import pyspark.sql.functions as F

from elmo_geo import LOG, register
from elmo_geo.datasets.datasets import parcels
from elmo_geo.io import download_link
from elmo_geo.utils.misc import dbfs

register()

# COMMAND ----------

parcels_names = sorted([f"{parcels.source}/{parcels.name}/{v.name}" for v in parcels.versions])
dbutils.widgets.dropdown("parcels", parcels_names[-1], parcels_names)
_, pname, pversion = dbutils.widgets.get("parcels").split("/")
[print("name", parcels.name, sep=":\t"), 
 print("version", next(v for v in parcels.versions if v.name == pversion), sep = ":\t"),
 ]

path_parcels = next(v.path_read for v in parcels.versions if v.name == pversion)

path_out = f"dbfs:/mnt/lab/restricted/ELM-Project/out/rpa-parcel-centroids-{pversion}.parquet"

# COMMAND ----------

df_parcels = (
    spark.read.format("geoparquet").load(path_parcels)
    .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
    .withColumn("centroid", F.expr("ST_AsText(ST_Centroid(geometry))"))
    .select("id_parcel", "centroid", "sindex")
)
df_parcels.display()

# COMMAND ----------

path_out

# COMMAND ----------

# download
pdf_parcels = df_parcels.toPandas()

# output
pdf_parcels.to_parquet(dbfs(path_out, False))
displayHTML(download_link(dbfs(path_out, False)))
