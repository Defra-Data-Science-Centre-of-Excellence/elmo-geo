# Databricks notebook source
# MAGIC %md
# MAGIC # Heritage Wall
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

from elmo_geo.st.st import join

# COMMAND ----------

sf_parcel = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet"
sf_wall = "dbfs:/mnt/lab/unrestricted/elm_data/osm/wall.parquet"

sdf_parcel = spark.read.parquet(sf_parcel).withColumn("geometry", fix_wkb())
sdf_wall = spark.read.parquet(sf_wall).withColumn("geometry", fix_wkb())

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
