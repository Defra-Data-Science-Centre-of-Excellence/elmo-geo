# Databricks notebook source
# MAGIC %md
# MAGIC # Heritage Wall
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from elmo_geo.io import load_missing
from elmo_geo.st import join

# COMMAND ----------

sf_parcel = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet"
sf_wall = "dbfs:/mnt/lab/unrestricted/elm_data/osm/wall.parquet"

sdf_parcel = spark.read.parquet(sf_parcel).withColumn("geometry", load_missing())
sdf_wall = spark.read.parquet(sf_wall).withColumn("geometry", load_missing())

# COMMAND ----------


# COMMAND ----------

sdf = join(
    spark.read.parquet(sf_parcel),
    spark.read.parquet(sf_wall),
    lsuffix="_parcel",
    rsuffix="_wall",
    distance=12,
)

display(sdf)

# COMMAND ----------

spark.rdd.RDD
