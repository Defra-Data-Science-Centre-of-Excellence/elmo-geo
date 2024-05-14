# Databricks notebook source
from pyspark.sql import functions as F
from sedona.register import SedonaRegistrator

SedonaRegistrator.registerAll(spark)

# COMMAND ----------

sf_parcel = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet/"

sf_out = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/boundary.parquet/"

# COMMAND ----------


def buf(x):
    return f"ST_Area(ST_Intersection(ST_MakeValid(ST_Buffer(ST_Boundary(geometry), {x})), geometry))/10000 AS ha_buf{x}"


sdf = (
    spark.read.parquet(sf_parcel)
    .filter("geometry IS NOT NULL")
    .select(
        "id_parcel",
        F.expr(buf(4)),
        F.expr(buf(6)),
        F.expr(buf(8)),
        F.expr(buf(10)),
        F.expr(buf(12)),
    )
)

display(sdf)
sdf.write.parquet(sf_out, mode="overwrite")
