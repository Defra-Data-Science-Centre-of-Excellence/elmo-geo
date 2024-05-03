# Databricks notebook source
import os
import re
import geopandas as gpd
from pyspark.sql import functions as F

from elmo_geo import LOG, register
from elmo_geo.datasets.datasets import datasets, parcels
from elmo_geo.io import download_link
from elmo_geo.io.preprocessing import geometry_to_wkb, make_geometry_valid, transform_crs
from elmo_geo.st import sjoin 
from elmo_geo.utils.misc import dbfs, info_sdf

register()

# COMMAND ----------

d_historic_england = "/mnt/base/unrestricted/source_historic_england_open_data_site"

sf_historic_england_template = "/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_{name1}/format_GEOPARQUET_{name1}/SNAPSHOT_2024_04_29_{name1}/layer={name2}.snappy.parquet"

sf_listed_buildings = sf_historic_england_template.format(name1="listed_buildings", name2="Listed_Buildings")
sf_protected_wreck_sites = sf_historic_england_template.format(name1="protected_wreck_sites", name2="Protected_Wreck_Sites")
sf_registered_battlefields = sf_historic_england_template.format(name1="registered_battlefields", name2="Registered_Battlefields")
sf_registered_parks_and_gardens = sf_historic_england_template.format(name1="registered_parks_and_gardens", name2="Registered_Parks_and_Gardens")
sf_scheduled_monuments = sf_historic_england_template.format(name1="scheduled_monuments", name2="Scheduled_Monuments")
sf_world_heritage_sites = sf_historic_england_template.format(name1="world_heritage_sites", name2="World_Heritage_Sites")

sf_shine = "/dbfs/mnt/lab/restricted/ELM-Project/stg/he-shine-2022_12_30.parquet"

date = "2024-05-03"
sf_output_historic_combined = f"/dbfs/mnt/lab/restricted/ELM-Project/stg/historic_archaeological_sites_combined-{date}.parquet"

# COMMAND ----------

paths = [
    sf_listed_buildings,
    sf_protected_wreck_sites,
    sf_registered_battlefields,
    sf_registered_parks_and_gardens,
    sf_scheduled_monuments,
    sf_world_heritage_sites,
]

for p in paths:
    print(p.split("/")[-1])
    spark.read.format("geoparquet").load(dbfs(p, True)).display()

# COMMAND ----------

# load shine data and rename columns to match other historic england datasets
sdf_historical_sites = (spark.read.format("parquet").load(dbfs(sf_shine, True))
                       .select(
                               F.col("shine_uid").alias("ListEntry"),
                               F.col("shine_name").alias("Name"),
                               F.col("geom").alias("geometry"),
                       )
                       .withColumn("dataset", F.lit("SHINE"))
                       )
sdf_historical_sites.display()

# COMMAND ----------

# Union datasets into single source of historical sites.
# sdf_historical_sites = (spark.read.format("geoparquet").load(dbfs(paths[0], True))
#                         .select("ListEntry", "Name", "geometry")
#                         .withColumn("dataset", F.lit(re.search(r"layer=(.*).snappy.parquet", paths[0]).groups()[0]))
# )

for p in paths:
    sdf_historical_sites = sdf_historical_sites.unionByName(
        spark.read.format("geoparquet").load(dbfs(p, True))
        .select("ListEntry", "Name", "geometry")
        .withColumn("geometry", F.expr("ST_AsBinary(geometry)"))
        .withColumn("dataset", F.lit(re.search(r"layer=(.*).snappy.parquet", p).groups()[0])),
        allowMissingColumns=False
    )

# COMMAND ----------

sdf_historical_sites.groupBy("dataset").agg(F.count("ListEntry")).display()

# COMMAND ----------

# save combined data to staging
sdf_historical_sites.write.format("parquet").save(sf_output_historic_combined)

# COMMAND ----------

# boundaries

