# Databricks notebook source
# MAGIC %md
# MAGIC # Providing data to EVAST
# MAGIC - Merge water and lookup parcels
# MAGIC - Verify water
# MAGIC - Download water and overlap datasets
# MAGIC
# MAGIC
# MAGIC [ceh_spol]: https://cehacuk.sharepoint.com/sites/EVAST/DEFRA%20Data%20Share/Forms/AllItems.aspx?id=%2Fsites%2FEVAST%2FDEFRA%20Data%20Share%2FIncoming%20Data%2FHedges%20And%20Water%20Body%20data%20Dec&viewid=2ea78def%2D1f62%2D4d01%2D9671%2D188ffdc282c6

# COMMAND ----------


from pyspark.sql import functions as F

from elmo_geo import register
from elmo_geo.io import download_link, to_gdf
from elmo_geo.plot.base_map import plot_gdf

register()

# COMMAND ----------

sdf_water = spark.read.parquet("dbfs:/mnt/lab/restricted/ELM-Project/silver/elmo_geo-water-2024_05_22.parquet")
sdf_lookup = spark.read.parquet("dbfs:/mnt/lab/restricted/ELM-Project/silver/lookup_parcel-water-2024_05_22.parquet")


gdf = to_gdf(sdf_water.join(sdf_lookup.groupby("fid").agg(F.collect_set("id_parcel").alias("id_parcel")), on="fid")).assign(
    id_parcel=lambda df: df["id_parcel"].str.join(",")
)


gdf.to_file("/tmp/elmo_geo-water-2024_05_22.gpkg")
gdf

# COMMAND ----------

# MAGIC %sh
# MAGIC cp /tmp/elmo_geo-water-2024_05_22.gpkg /dbfs/FileStore/elmo_geo-water-2024_05_22.gpkg
# MAGIC du -h /tmp/elmo_geo-water-2024_05_22.gpkg
# MAGIC du -h /dbfs/FileStore/elmo_geo-water-2024_05_22.gpkg

# COMMAND ----------

download_link("/dbfs/FileStore/elmo_geo-water-2024_05_22.gpkg")

# COMMAND ----------

plot_gdf(gdf)

# COMMAND ----------

# MAGIC %ls /dbfs/mnt/lab/restricted/ELM-Project/silver/overlap*

# COMMAND ----------

spark.read.parquet("dbfs:/mnt/lab/restricted/ELM-Project/silver/overlap-water-2024_05_22.parquet").display()

# COMMAND ----------

spark.read.parquet("dbfs:/mnt/lab/restricted/ELM-Project/silver/overlap-class_water-2024_05_22.parquet").display()
