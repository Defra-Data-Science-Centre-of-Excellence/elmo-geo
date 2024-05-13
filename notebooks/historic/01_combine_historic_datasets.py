# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Combine Historic and Archaeological Datasets
# MAGIC **Author:** Obi Thompson Sargoni
# MAGIC
# MAGIC **Date:** 13-05-2024
# MAGIC
# MAGIC This notebook combines multiple sources of historic and archaeological features into a single parquet file.
# MAGIC
# MAGIC This enables the proportion of parcels intersected by historic or archaeological features to be calculated, though this is done in a separate notebooks.

# COMMAND ----------

import os
import re
import geopandas as gpd
from pyspark.sql import functions as F

from elmo_geo import LOG, register
from elmo_geo.datasets.datasets import datasets, parcels
from elmo_geo.io import download_link
from elmo_geo.io.preprocessing import geometry_to_wkb, make_geometry_valid, transform_crs
from elmo_geo.st import sjoin
from elmo_geo.st.geometry import load_geometry
from elmo_geo.st.udf import st_union
from elmo_geo.utils.misc import dbfs, info_sdf

register()

# COMMAND ----------

sf_historic_england_template = "/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_{name1}/format_GEOPARQUET_{name1}/SNAPSHOT_{snapshot_date}_{name1}/layer={name2}.snappy.parquet"

sf_protected_wreck_sites = sf_historic_england_template.format(name1="protected_wreck_sites", name2="Protected_Wreck_Sites", snapshot_date = "2024_04_29")
sf_registered_battlefields = sf_historic_england_template.format(name1="registered_battlefields", name2="Registered_Battlefields", snapshot_date = "2024_04_29")
sf_registered_parks_and_gardens = sf_historic_england_template.format(name1="registered_parks_and_gardens", name2="Registered_Parks_and_Gardens", snapshot_date = "2024_04_29")
sf_scheduled_monuments = sf_historic_england_template.format(name1="scheduled_monuments", name2="Scheduled_Monuments", snapshot_date = "2024_04_29")
sf_world_heritage_sites = sf_historic_england_template.format(name1="world_heritage_sites", name2="World_Heritage_Sites", snapshot_date = "2024_04_29")
sf_listed_buildings = sf_historic_england_template.format(name1="listed_buildings_polys", name2="Listed_Buildings_polygons", snapshot_date = "2024_05_03")

sf_shine = "/dbfs/mnt/lab/restricted/ELM-Project/stg/he-shine-2022_12_30.parquet"

date = "2024_05_03"
sf_output_historic_combined = f"/dbfs/mnt/lab/restricted/ELM-Project/stg/he-combined_sites-{date}.parquet"

# COMMAND ----------

paths = [
    #sf_listed_buildings, # TODO: Include listed buildings once available: https://github.com/Defra-Data-Science-Centre-of-Excellence/elmo-geo/issues/126
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
sdf_shine_sites = spark.read.format("parquet").load(dbfs(sf_shine, True))
sdf_shine_sites.display()

# COMMAND ----------

sdf_historical_sites = (sdf_shine_sites
                       .select(
                               F.col("shine_uid").alias("ListEntry"),
                               F.col("shine_name").alias("Name"),
                               F.col("geom").alias("geometry"),
                       )
                       .withColumn("dataset", F.lit("SHINE"))
                       )

for p in paths:
    sdf_historical_sites = sdf_historical_sites.unionByName(
        spark.read.format("geoparquet").load(dbfs(p, True))
        .select("ListEntry", "Name", "geometry")
        .withColumn("geometry", F.expr("ST_AsBinary(geometry)"))
        .withColumn("dataset", F.lit(re.search(r"layer=(.*).snappy.parquet", p).groups()[0])),
        allowMissingColumns=False
    )

# prepare for dissolving overlapping geometries
sdf_historical_sites = (sdf_historical_sites
                        .withColumn("geometry", load_geometry("geometry"))
                        .withColumn("geometry", F.expr("EXPLODE(ST_Dump(geometry))")) # convert any multi parts to single parts
                        .repartition(100)
)

(sdf_historical_sites
 .withColumn("geometry", F.expr("ST_AsBinary(geometry)"))
 .write.format("parquet").mode("overwrite").save(dbfs(sf_output_historic_combined, True))
)

# COMMAND ----------

spark.read.format("parquet").load(sf_output_historic_combined).display()

# COMMAND ----------

# report numbers of sites
sdf_historical_sites.groupBy("dataset").agg(F.count("ListEntry")).display()

# report geometry types
(sdf_historical_sites
 .withColumn("gtype", F.expr("ST_GeometryType(geometry)"))
 .groupby("gtype")
 .agg(F.count("geometry"))
).display()
