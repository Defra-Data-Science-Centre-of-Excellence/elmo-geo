# Databricks notebook source
# MAGIC %md
# MAGIC # OSM Download to Parquet
# MAGIC Download datasets from OpenStreetMap, and save as parquet.
# MAGIC
# MAGIC ### Data
# MAGIC - Hedgerows = barrier:[hedge,hedge_bank],landcover:hedge
# MAGIC - Waterbodies = water:true,waterway:true
# MAGIC - Heritage = wall:dry_stone
# MAGIC

# COMMAND ----------

# MAGIC %pip install -q osmnx

# COMMAND ----------

import osmnx as ox
from pyspark.sql import functions as F
from sedona.register import SedonaRegistrator

ox.settings.cache_folder = "/dbfs/tmp/"

SedonaRegistrator.registerAll(spark)

# COMMAND ----------


def st_fromwkb(col: str = "geometry", from_crs: int = None):
    return F.expr(
        f'ST_SimplifyPreserveTopology(ST_PrecisionReduce(ST_Transform(ST_FlipCoordinates(ST_Force_2D(CASE WHEN ({col} IS NULL) THEN ST_GeomFromText("Point EMPTY") ELSE ST_MakeValid(ST_GeomFromWKB({col})) END)), "EPSG:{from_crs}", "EPSG:27700"), 3), 0)'
    )  # noqa:E501


def osm2sdf(place, tags):
    return (
        ox.geometries_from_place(place, tags)
        .reset_index()[["osmid", *tags.keys(), "geometry"]]
        .to_wkb()
        .pipe(spark.createDataFrame)
        .withColumn("geometry", st_fromwkb("geometry", from_crs=4326))
    )


# COMMAND ----------

sf_osm_hedge = "dbfs:/mnt/lab/unrestricted/elm_data/osm/hedgerows.parquet"
sf_osm_water = "dbfs:/mnt/lab/unrestricted/elm_data/osm/waterbodies.parquet"
sf_osm_heritage = "dbfs:/mnt/lab/unrestricted/elm_data/osm/heritage.parquet"

# COMMAND ----------

place = "England"
tags = {
    "barrier": ["hedge", "hedge_bank"],
    "landcover": "hedge",
}

sdf_osm_hedge = osm2sdf(place, tags)

sdf_osm_hedge.write.parquet(sf_osm_hedge)
display(sdf_osm_hedge)

# COMMAND ----------

place = "England"
tags = {
    "water": True,
    "waterway": True,
}

sdf_osm_water = osm2sdf(place, tags)


sdf_osm_water.write.parquet(sf_osm_water)
display(sdf_osm_water)

# COMMAND ----------

place = "England"
tags = {
    "wall": "dry_stone",
}

sdf_osm_heritage = osm2sdf(place, tags)


sdf_osm_heritage.write.parquet(sf_osm_heritage)
display(sdf_osm_heritage)
