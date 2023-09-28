# Databricks notebook source
from glob import glob

from pyspark.sql import Window
from pyspark.sql import functions as F

import elmo_geo

elmo_geo.register()

# COMMAND ----------


# COMMAND ----------

LETTERS = "ABCDEFGHJKLMNOPQRSTUVWXYZ"  # string.ascii_uppercase.replace('I', '')
MINX = -10_000_000
MINY = -10_000_000
GRID = 5_000_000, 5_000
XA = 0, 0


# COMMAND ----------

sf = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_06_03.geoparquet"
sdf = (
    spark.read.parquet(sf)
    .repartition(2000, "SHEET_ID")
    .select(
        F.concat("SHEET_ID", "PARCEL_ID").alias("id_parcel"),
        elmo_geo.io.io2.load_geometry("geometry").alias("geometry"),
    )
)

# sf = 'dbfs:/tmp/awest/tmp.parquet'
print(sdf.rdd.getNumPartitions())
# sdf.write.parquet(sf)
display(sdf)

# COMMAND ----------

# DBTITLE 1,GeoHash Partitioning
sf = "dbfs:/tmp/awest/tmp.parquet"
sdf = (
    spark.read.parquet(sf)
    .withColumn(
        "sindex",
        F.expr(
            'ST_GeoHash(ST_FlipCoordinates(ST_Transform(geometry, "EPSG:27700", "EPSG:4326")), 4)'
        ),
    )
    .withColumn(
        "sindex_batch",
        (F.row_number().over(Window.partitionBy("sindex").orderBy("sindex")) / 10_000).cast("int"),
    )
    .repartition("sindex", "sindex_batch")
)

sdf.select("sindex", "sindex_batch").distinct().count()

# COMMAND ----------

glob("/dbfs/mnt/base/unrestricted/source_*[!bluesky]/**/*.csv", recursive=True)

# COMMAND ----------

# MAGIC %ls -lh '/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_nitrate_vulnerable_zones_2017/format_GPKG_nitrate_vulnerable_zones_2017/LATEST_nitrate_vulnerable_zones_2017/nvz_2017.csv.csv'  # noqa:E501
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE table1 ();
# MAGIC
# MAGIC
# MAGIC COPY INTO table1
# MAGIC   FROM "dbfs:/mnt/base/unrestricted/source_defra_data_services_platform/dataset_nitrate_vulnerable_zones_2017/format_GPKG_nitrate_vulnerable_zones_2017/LATEST_nitrate_vulnerable_zones_2017/nvz_2017.csv.csv"  # noqa:E501
# MAGIC   FILEFORMAT = csv
# MAGIC   FORMAT_OPTIONS('header'='true', 'inferSchema'='True');
# MAGIC
# MAGIC SELECT *
# MAGIC FROM table1;
