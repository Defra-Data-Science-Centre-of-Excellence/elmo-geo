# Databricks notebook source
# MAGIC %md
# MAGIC # Parcels
# MAGIC Create a simple parcel geometry dataset, including business id.
# MAGIC Join WFM to Reference Parcels, and simplify the geometry data with Sedona.
# MAGIC
# MAGIC ### Input Data
# MAGIC - LEEP, WFM, v3
# MAGIC   `dbfs:/mnt/lab/unrestricted/elm/wfm/v3.parquet`
# MAGIC - RPA, Reference Parcels, 2023-02-07
# MAGIC   `dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_02_07.parquet`
# MAGIC
# MAGIC ### Output Data
# MAGIC - ELM, Buffer Strips, Parcels
# MAGIC   `dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from sedona.register import SedonaRegistrator

SedonaRegistrator.registerAll(spark)

# COMMAND ----------


def st_fromwkb(col: str = "geometry", from_crs: int = None):
    return F.expr(
        f'ST_SimplifyPreserveTopology(ST_PrecisionReduce(ST_Transform(ST_Force_2D(CASE WHEN ({col} IS NULL) THEN ST_GeomFromText("Point EMPTY") ELSE ST_MakeValid(ST_GeomFromWKB({col})) END), "EPSG:{from_crs}", "EPSG:27700"), 3), 0)',  # noqa:E501
    )


# COMMAND ----------

sf_wfm = "dbfs:/mnt/lab/unrestricted/elm/wfm/v3.parquet"
sf_parcels = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_02_07.parquet"
sf_out = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet"

# COMMAND ----------

sdf = SparkDataFrame.join(
    spark.read.parquet(sf_wfm).select(
        "id_business",
        "id_parcel",
    ),
    spark.read.parquet(sf_parcels).select(
        F.concat("SHEET_ID", "PARCEL_ID").alias("id_parcel"),
        st_fromwkb("wkb_geometry", from_crs=27700).alias("geometry"),
    ),
    how="left",
    on="id_parcel",
)

sdf.write.parquet(sf_out)
display(sdf)
