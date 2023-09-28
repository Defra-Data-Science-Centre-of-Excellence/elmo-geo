# Databricks notebook source
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
from shapely import from_wkt, from_wkb

from elm_se import st, io, types, register
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
from sedona.register import SedonaRegistrator
SedonaRegistrator.registerAll(spark)

# COMMAND ----------

sf_tow_sp = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_SP_England_26062023.parquet"
sf_tow_lidar = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_LiDAR_England_26062023.parquet"



sdf_tow_li = spark.read.parquet(sf_tow_lidar) # TOW data came as two geodatabases - lidar and sp
sdf_tow_sp = spark.read.parquet(sf_tow_sp)
sdf_tow = (sdf_tow_li # Combine two two datasets
              .union(
                  sdf_tow_sp.select(*list(sdf_tow_li.columns)) # sdf_tow_sp has 4 additional columns that we don't want
                  )
              .withColumn("geometry", io.load_geometry(column='geometry'))
)

# COMMAND ----------

sf_tow_dissolved = "dbfs:/mnt/lab/unrestricted/elm/forest_research/tow_dissolved/TOW_England_26062023.parquet"
sf_tow_dissolved_sp = "dbfs:/mnt/lab/unrestricted/elm/forest_research/tow_dissolved/TOW_SP_26062023.parquet"

# COMMAND ----------

gdf_100kmgrid = gpd.read_file("os_bng_grids.gpkg", layer = '100km_grid')
sp_polygon = gdf_100kmgrid.loc[ gdf_100kmgrid['tile_name']=='SP', 'geometry'].values[0]

# COMMAND ----------

gdf_100kmgrid.shape

# COMMAND ----------

sdf_dissolved = (sdf_tow
                 .repartition(10_000, "LiDAR_Tile", "Woodland_Type_V2")
                 .filter(F.expr(f"ST_Intersects(geometry, ST_GeomFromText('{sp_polygon.wkt}'))"))
                 #.withColumn("geometry", F.expr("ST_GeomFromWKB(geometry)"))
                 .groupby('LiDAR_Tile', "Woodland_Type_V2")
                 .agg(F.expr(f'ST_Union_Aggr(geometry)  AS geometry'))
)

# COMMAND ----------

sdf_dissolved.write.mode("overwrite").parquet(sf_tow_dissolved_sp)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Compare area of dissolved TOW to undissolved

# COMMAND ----------

area_undissolved = (sdf_tow
                   .repartition(10_000)
                   .filter(F.expr(f"ST_Intersects(geometry, ST_GeomFromText('{sp_polygon.wkt}'))"))
                   .select(F.expr("ST_Area(geometry) as area"))
                   .agg(F.sum("area")).collect()[0][0]
)
area_undissolved

# COMMAND ----------

area_dissolved = (spark.read.parquet(sf_tow_dissolved_sp)
                 .select(F.expr("ST_Area(geometry) as area"))
                 .agg(F.sum("area")).collect()[0][0]
)

area_dissolved

# COMMAND ----------

(area_undissolved - area_dissolved) / area_undissolved
