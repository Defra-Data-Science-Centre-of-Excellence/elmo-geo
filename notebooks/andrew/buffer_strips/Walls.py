# Databricks notebook source
# MAGIC %md
# MAGIC # Walls from OSM
# MAGIC Download OSM England walls data using:  osmnx
# MAGIC Find Tags using: [http://tagfinder.herokuapp.com/](http://tagfinder.herokuapp.com/)
# MAGIC
# MAGIC walls include "dry stone walls" and "Cornish hedges" (no OSM data for these).
# MAGIC
# MAGIC
# MAGIC Hedgerows, Walls, Waterbodies, Woodland, Buffers 4-12m

# COMMAND ----------

# MAGIC %pip install -q osmnx contextily git+https://github.com/aw-west-defra/cdap_geo.git

# COMMAND ----------

import contextily as ctx
import geopandas as gpd
import osmnx as ox

# Repo
ox.settings.cache_folder = "/dbfs/tmp/"

# SSL Issue
import urllib3

urllib3.disable_warnings()
ox.settings.requests_kwargs = {"verify": False}

from cdap_geo.sedona import st_join, st_load, st_register, st_valid
from pyspark.sql import functions as F
from pyspark.sql import types as T

st_register()


from sedona.sql.types import GeometryType

buffer = (
    gpd.tools.util.BaseGeometry.buffer if not hasattr(shapely, "buffer") else shapely.buffer
)  # osmnx dependency issue


def st_buffer_udf(g, res, **kwargs):
    """Created for cap_style='flat'"""

    @F.udf(returnType=GeometryType())
    def _st_buffer_udf(g):
        return buffer(g, res, **kwargs)

    return _st_buffer_udf(g)


crs = 27700
key = "WxgUdETn6cy58WZkfwZ7wdMVLlt5eDsX"
basemap_url = "https://api.os.uk/maps/raster/v1/zxy/Light_3857/{z}/{x}/{y}.png?key=" + key

# COMMAND ----------

sf_parcel = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet"
sf_wall = "dbfs:/mnt/lab/unrestricted/elm_data/osm/wall.parquet"

sf_geoms = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/wall_geometries.parquet"
sf_out = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/wall.parquet"

# COMMAND ----------

place = "England"
tags = {"wall": "dry_stone"}


# Download
df_england = ox.geocode_to_gdf(place).to_crs("epsg:27700")
df_wall = (
    ox.geometries_from_place(place, tags)
    .to_crs("epsg:27700")
    .reset_index()[["osmid", "geometry"]]
    .query('geometry.geom_type == "LineString"')
)


# Save
sdf_wall = spark.createDataFrame(df_wall)
sdf_wall.write.parquet(sf_wall, mode="overwrite")


# Plot
ax = df_wall.plot(figsize=[16, 9], color="C4")
ctx.add_basemap(ax=ax, crs=crs, source=basemap_url)

display(sdf_wall)

# COMMAND ----------

df_parcel = spark.read.parquet(sf_parcel)
df_wall = spark.read.parquet(sf_wall)


sdf_geoms = (
    st_join(
        df_parcel,
        (
            df_wall.withColumnRenamed("geometry", "geometry_wall").withColumn(
                "geometry", F.expr("ST_Buffer(geometry_wall, 2)")
            )
        ),
        lsuffix="_parcel",
        rsuffix="",
    )
    .drop("geometry")
    .withColumn("geometry_hedge", F.explode(F.expr("ST_Dump(geometry_wall)")))
    .groupby("id_parcel")
    .agg(
        F.first("id_business").alias("id_business"),
        F.collect_set("osmid").alias("osmid"),
        F.first("geometry_parcel").alias("geometry_parcel"),
        F.expr("ST_SimplifyPreserveTopology(ST_Union_Aggr(geometry_wall), 0.001)").alias(
            "geometry_wall"
        ),
    )
)


sdf_geoms.write.parquet(sf_geoms, mode="overwrite")
display(sdf_geoms)

# COMMAND ----------

sdf = (
    spark.read.parquet(sf_geoms)
    .withColumn("buf", F.expr("ST_MakeValid(ST_Buffer(geometry_wall, 0.001))"))
    .withColumn(
        "uncapped_buf2", F.expr("ST_Buffer(buf, 2)")
    )  # st_buffer_udf('buf', 2, cap_style='flat'))
    .select(
        "id_business",
        "id_parcel",
        F.expr("ST_Length(geometry_parcel)").alias("m_parcel_boundary"),
        F.expr("ST_Length(geometry_wall)").alias("m_wall"),
        F.expr("ST_Length(ST_Difference(ST_Boundary(geometry_parcel), uncapped_buf2))").alias(
            "m_none_wall_boundary"
        ),
        F.expr("ST_Area(ST_MakeValid(ST_Buffer(buf, 12)))").alias("sqm_buf12_unclipped"),
        F.expr("ST_Area(ST_Intersection(ST_MakeValid(ST_Buffer(buf, 12)), geometry_parcel))").alias(
            "sqm_buf12"
        ),
        F.expr("ST_Area(ST_Intersection(ST_MakeValid(ST_Buffer(buf, 10)), geometry_parcel))").alias(
            "sqm_buf10"
        ),
        F.expr("ST_Area(ST_Intersection(ST_MakeValid(ST_Buffer(buf, 8)), geometry_parcel))").alias(
            "sqm_buf8"
        ),
        F.expr("ST_Area(ST_Intersection(ST_MakeValid(ST_Buffer(buf, 6)), geometry_parcel))").alias(
            "sqm_buf6"
        ),
        F.expr("ST_Area(ST_Intersection(ST_MakeValid(ST_Buffer(buf, 4)), geometry_parcel))").alias(
            "sqm_buf4"
        ),
    )
)


sdf.write.parquet(sf_out, mode="overwrite")
display(sdf)

# COMMAND ----------

display(spark.read.parquet(sf_out))

# COMMAND ----------

sdf.select(F.sum("m_parcel_boundary"), F.sum("m_wall"), F.sum("m_none_wall_boundary")).display()
