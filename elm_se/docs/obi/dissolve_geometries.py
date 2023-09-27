# Databricks notebook source
# MAGIC %pip install contextily

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd
import shapely
import geopandas as gpd
import contextily as ctx
import itertools
from shapely.geometry import Polygon
from shapely import from_wkt, from_wkb

from elm_se import st, io, types, register
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
from sedona.register import SedonaRegistrator
SedonaRegistrator.registerAll(spark)

# COMMAND ----------

def setup_geoplot(lims = (446450, 349550, 446650, 349850), basemap = True, figsize = (16,9)):
  fig, ax = plt.subplots(figsize=figsize)
  ax.axis('off')
  if lims is not None:
      ax.set(xlim=[lims[0], lims[2]], ylim=[lims[1], lims[3]])
  if basemap:
      ctx.add_basemap(ax=ax, source=ctx.providers.Thunderforest.Landscape(apikey='25a2eb26caa6466ebc5c2ddd50c5dde8', attribution=None), crs='EPSG:27700')
  return fig, ax

# COMMAND ----------

path_out = 'dbfs:/mnt/lab/unrestricted/elm/elm_se/testSP5833/elm_se-{}-2023_08.parquet'
sf_parcels_out = path_out.format('parcels')  # Simplified: id_business, id_parcel, geometry
sf_parcel_segments_out = path_out.format('parcel_segments')  # Simplified: id_business, id_parcel, geometry
sf_hedgerows_out = path_out.format('hedgerows')  # Joined: id_parcel, source, class, geometry
sf_woodland_out = path_out.format('woodland')  # Joined: id_parcel, source, class, geometry
sf_other_woody_tow_out = path_out.format('other_woody_tow')  # Joined: id_parcel, source, class, geometry
sf_other_woody_vom_out = path_out.format('other_woody_vom_td')  # Joined: id_parcel, source, class, geometry
sf_woody_boundaries_out = path_out.format('woody_boundaries')  # Priority Method + Relict: id_parcel, g_hedge, g_woodland, g_relict
sf_boundary_uses_out = path_out.format('boundary_uses')  # Splitting Method: id_parcel, bools, m
sf_relict_segments_out = path_out.format('relict_segments')  # Uses joined to parcel segments. Relict classification applied to filter out non-relict segments.
sf_vom_td = f"dbfs:/mnt/lab/unrestricted/elm/elmo/hrtrees/tree_detections/tree_detections_202308040848.parquet"
sf_tow_sp = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_SP_England_26062023.parquet"
sf_tow_lidar = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_LiDAR_England_26062023.parquet"

sdf_parcel_segments = spark.read.parquet(sf_parcel_segments_out)
sdf_tow_li = spark.read.parquet(sf_tow_lidar) # TOW data came as two geodatabases - lidar and sp
sdf_tow_sp = spark.read.parquet(sf_tow_sp)
sdf_tow = (sdf_tow_li # Combine two two datasets
              .union(
                  sdf_tow_sp.select(*list(sdf_tow_li.columns)) # sdf_tow_sp has 4 additional columns that we don't want
                  )
              .withColumn("geometry", io.load_geometry(column='geometry'))
)

sdf_other_woody_tow = spark.read.parquet(sf_other_woody_tow_out)

# COMMAND ----------

# Convert udt to wkb - required to be able to read data in new workspace
import os

def sdf_to_wkb(sdf):
    for column in sdf.columns:
        if isinstance(sdf.schema[column].dataType, types.SedonaType):
            sdf = sdf.withColumn(column, F.expr(f'ST_AsBinary({column})'))
    return sdf

d = "/dbfs/mnt/lab/unrestricted/elm/elm_se/testSP5833/"
for f in os.listdir(d):
    if f.split(".")[-1] == 'parquet':
        print(f)
        path = os.path.join(d.replace("/dbfs", "dbfs:"), f)
        sdf = spark.read.parquet(path)
        sdf = sdf_to_wkb(sdf)
        sdf.write.mode("overwrite").parquet(path)

# COMMAND ----------

# Try union aggregating geometries here then saving to file
def group_agg_features(sdf, right_geom):
  sdf = (sdf
    .groupby('bng_10km', 'bng_1km', 'id_parcel', 'id_segment')  # SparkDF.GroupBy takes *args (instead of list)
    #.agg(F.expr(f'ST_Union_Aggr({right_geom})  AS geometry_tmp')) # fails
    #.agg(F.expr(f'ST_Union_Aggr(ST_PrecisionReduce({right_geom}, 3))  AS geometry_tmp')) # fails
    .agg(F.expr(f'ST_Union_Aggr(ST_PrecisionReduce(ST_MakeValid({right_geom}), 3))  AS geometry_tmp')) # fails
    #.agg(F.expr(f'ST_Envelope_Aggr({right_geom})  AS geometry_tmp')) # succeeds
  )
  return sdf

sf_other_woody_tow_relict_out = path_out.format('other_woody_tow_relict')

# Try to reproduce with smaller area
sdf_other_woody_tow_relict = (sdf_other_woody_tow
                              #.filter(F.expr("bng_10km in ('SP53')")) # failed 
                              #.filter(F.expr("bng_1km in ('SP5833')")) # failed SP5833
                              .filter(F.expr( "( (Woodland_Type_V2 not in ('Small woods', 'Group of trees') ) or (Average_Width < 5 ))"))
)
sdf_other_woody_tow_relict = group_agg_features(sdf_other_woody_tow_relict, 'geometry_tow')
sdf_other_woody_tow_relict.repartition(2000, "bng_10km").write.mode("overwrite").parquet(sf_other_woody_tow_relict_out)

# COMMAND ----------

# Filter tow daat to just the study area
# Select just the geometries causing the issue
bbox = np.array([458495, 233478, 458500, 233482]).reshape(2,-1)
l = list(itertools.product(bbox[:, 0], bbox[:, 1]))
order = [0,1,3,2, 0]
l = [l[_ind] for _ind in order]
polygon = Polygon(l)

# COMMAND ----------

sdf_tow = (sdf_tow
           .filter(F.expr(f"ST_Intersects(geometry, ST_GeomFromText('{polygon.wkt}'))"))
)

gdf_tow = io.SparkDataFrame_to_PandasDataFrame(sdf_tow
)
gdf_tow = gpd.GeoDataFrame(gdf_tow, geometry = gpd.GeoSeries.from_wkb(gdf_tow['geometry'], crs=27700), crs = 27700 )

# COMMAND ----------

gdf_tow.to_file("tow_data.gpkg", driver = 'GPKG')

# COMMAND ----------

gdf_parcel_segments = io.SparkDataFrame_to_PandasDataFrame(sdf_parcel_segments
)
gdf_parcel_segments = gpd.GeoDataFrame(gdf_parcel_segments, geometry = gpd.GeoSeries.from_wkb(gdf_parcel_segments['geometry'], crs=27700), crs = 27700 )

gdf_other_woody_tow = io.SparkDataFrame_to_PandasDataFrame(sdf_other_woody_tow
)
gdf_other_woody_tow = gpd.GeoDataFrame(gdf_other_woody_tow, geometry = gpd.GeoSeries.from_wkb(gdf_other_woody_tow['geometry_tow'], crs=27700), crs = 27700 )

# COMMAND ----------

gdf_other_woody_tow.loc[:, ['geometry']].to_file("debug_tow_data.gpkg", driver = 'GPKG')

# COMMAND ----------

f, ax = setup_geoplot(lims = None, basemap = False, figsize = (10,10))
gdf_tow.plot(ax = ax, alpha = 0.5)

# COMMAND ----------

gdf_tow['key'] = 1
gdf_dis = gdf_tow.dissolve(by = 'Woodland_Type_V2')

gdf_dis

# COMMAND ----------

f, ax = setup_geoplot(lims = None, basemap = False, figsize = (10,10))
gdf_dis.iloc[:1].plot(ax = ax, alpha = 0.5)
gdf_dis.iloc[1:].plot(ax = ax, alpha = 0.5, color='red')

# COMMAND ----------

# Create spark dataframe from this gdf and develop dissolve method in spark
sdf = io.PandasDataFrame_to_SparkDataFrame(gdf_tow.to_wkb(), spark=spark)
sdf = sdf.withColumn("geometry", io.load_geometry("geometry"))

# COMMAND ----------

sdf_agg = (sdf
           .groupby('Woodland_type_V2')
           .agg(F.expr(f'ST_Union_Aggr(geometry)  AS geometry'))
)
sf_tow_agg = path_out.format('debug_tow_agg')
sdf_agg.write.mode("overwrite").parquet(sf_tow_agg)

# COMMAND ----------

sdf.display()

# COMMAND ----------

sdf_agg_segments = st.sjoin(sdf_parcel_segments, sdf_agg, lsuffix = '_seg', rsuffix = '', spark = spark)

(sdf_agg_segments
    .groupby('bng_10km', 'bng_1km', 'id_parcel', 'id_segment')  # SparkDF.GroupBy takes *args (instead of list)
    .agg(F.expr(f'ST_Union_Aggr(geometry)  AS geometry_tmp'))
    .display()
)

# COMMAND ----------

sf_tow_agg = path_out.format('debug_tow_agg')
sdf_agg = spark.read.parquet(sf_tow_agg)

# COMMAND ----------

gdf_agg = io.SparkDataFrame_to_PandasDataFrame(sdf_agg)
gdf_agg = gpd.GeoDataFrame(gdf_agg, geometry = gpd.GeoSeries.from_wkb(gdf_agg['geometry'], crs=27700), crs = 27700 )

f, ax = setup_geoplot(lims = None, basemap = False, figsize = (10,10))
gdf_agg.iloc[:1].plot(ax = ax, alpha = 0.5)
gdf_agg.iloc[1:].plot(ax = ax, alpha = 0.5, color='red')

# COMMAND ----------

# Check the intersection between these two geometries
# - there is a slight intersection
# - not big enough to be an issue for the analysis but could lead to more topology errors
g_hr = gdf_agg.iloc[0]['geometry']
g_tr = gdf_agg.iloc[1]['geometry']

g_hr.intersection(g_tr).area

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Visualise LiDAR and SP layers separaterly

# COMMAND ----------

# If is load just LiDAR / SP separately

# COMMAND ----------

sdf_tow_li = sdf_tow_li.repartition(10_000)

# COMMAND ----------

sdf_tow_li = (sdf_tow_li
              .withColumn("geometry", io.load_geometry("geometry"))
           .filter(F.expr(f"ST_Intersects(geometry, ST_GeomFromText('{polygon.wkt}'))"))
)

gdf_tow_li = io.SparkDataFrame_to_PandasDataFrame(sdf_tow_li)
gdf_tow_li = gpd.GeoDataFrame(gdf_tow_li, geometry = gpd.GeoSeries.from_wkb(gdf_tow_li['geometry'], crs=27700), crs = 27700 )

# COMMAND ----------

gdf_tow_li.to_file("tow_li_data.gpkg", driver = 'GPKG')

# COMMAND ----------

gdf_tow_li.shape

# COMMAND ----------

polygon.wkt

# COMMAND ----------

gdf_tow_li

# COMMAND ----------

sdf_tow_li = io.PandasDataFrame_to_SparkDataFrame(gdf_tow_li.to_wkb(), spark=spark)

# COMMAND ----------

# Aggregating the geometries to dissolve
sdf_agg = (sdf_tow_li
           .withColumn("geometry", F.expr("ST_GeomFromWKB(geometry)"))
           .groupby('LiDAR_Tile')
           .agg(F.expr(f'ST_Union_Aggr(geometry)  AS geometry'))
)

gdf_agg = io.SparkDataFrame_to_PandasDataFrame(sdf_agg)
gdf_agg = gpd.GeoDataFrame(gdf_agg, geometry = gpd.GeoSeries.from_wkb(gdf_agg['geometry'], crs=27700), crs = 27700 )

f, ax = setup_geoplot(lims = None, basemap = False, figsize = (10,10))
gdf_agg.plot(ax = ax, alpha = 0.5)
#gdf_agg.iloc[1:].plot(ax = ax, alpha = 0.5, color='red')

# COMMAND ----------

gdf_agg

# COMMAND ----------


