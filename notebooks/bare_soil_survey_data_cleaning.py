# Databricks notebook source
# MAGIC %md # Bare Soil Survey Comparison
# MAGIC We have received survey data so we can evaluate model analysis. We would like to see whether the satelitte data lines up with the survey data that we have got. 
# MAGIC 
# MAGIC In this notwbook, we will:
# MAGIC - Locate data required from 3 sources: Parcel geometries, point of survey and NDVI calculated for point
# MAGIC - overlay points to see which parcels have survey points located inside
# MAGIC - Find NDVI calculated for each point

# COMMAND ---------- 
```{bash}
%load_ext autoreload
%autoreload 2
```

# COMMAND ----------

# MAGIC %pip install -qU git+https://github.com/aw-west-defra/cdap_geo.git # andrews python package

# COMMAND ----------

import os
import re
import pandas as pd
import geopandas as gpd
import numpy as np
from cdap_geo import to_gdf, st_join # https://github.com/aw-west-defra/cdap_geo
from pyspark.sql import functions as F, types as T

# rioxarray stuff
import xarray as xr
from xarray.core.dataarray import DataArray
import rioxarray as rxr
import rasterio
from shapely.geometry import Polygon
from rioxarray.exceptions import NoDataInBounds, OneDimensionalRaster

from sedona.register import SedonaRegistrator
SedonaRegistrator.registerAll(spark)

# COMMAND ----------

# locating data
path_parcels = "/mnt/lab/unrestricted/elm/elmo/baresoil/parcels.parquet"
path_surveys = "/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/survey_data"
year = 2023
month_fm = f"{year-1}-11"
month_to = f"{year}-02"


files = [f for f in os.listdir(path_surveys)]
print(f"Found {len(files)} matching files")


# COMMAND ----------

files_paths = [f"{path_surveys.replace('/dbfs','')}/{f}" for f in files]
# files_iter = iter(files_paths)
# df = spark.read.parquet(next(files_iter))
# for f in files_iter:
#     df = df.union(spark.read.parquet(f))
# df.show()

df_survey1 = spark.read.csv((files_paths[0]),header=True, inferSchema=True).drop('Groundcover-notes_Quadrats')
df_survey2 = spark.read.csv((files_paths[1]),header=True, inferSchema=True)

surveys_df = df_survey1.union(df_survey2)

# COMMAND ----------

survey_df = (surveys_df
  .withColumnRenamed('Groundcover-QuadratLocation-Latitude', 'latitude')
  .withColumnRenamed('Groundcover-QuadratLocation-Longitude', 'longitude')
  .filter('latitude is not null AND longitude is not null')
  .select(
    F.col('Farm-FieldRLR_Number').alias('Farm_number'),
    (F.col('Groundcover-BareGround')/100).alias('bareground_percent_survey'),
    F.expr('ST_Transform(ST_Point(latitude, longitude), "epsg:4326","epsg:27700")').alias('geometry') 
  )
)
display(survey_df)

# COMMAND ----------

# getting parcels compatible for survey

parcel_df = spark.read.parquet(path_parcels)

parcel_df = (
    parcel_df
    .select('id_parcel','tile','geometry')
    .withColumn('geometry', F.expr('ST_GeomFromWKB(hex(geometry))'))
    .withColumnRenamed('geometry','geometry_polygon')
)
display(parcel_df)

# COMMAND ----------

# Combining parcel and survey data frames to check points inside each tile
cross_df = parcel_df.join(survey_df,how='cross')

cross_df = cross_df.withColumn('is_within', F.expr(f'ST_Within(geometry,geometry_polygon)'))

cross_df = cross_df.filter(cross_df.is_within==True)

display(cross_df)

# COMMAND ----------

df = (cross_df
  .withColumn('geometry', F.expr('ST_AsBinary(geometry)'))
  .withColumn('geometry_polygon', F.expr('ST_AsBinary(geometry_polygon)'))
  .toPandas()
  .pipe(lambda pdf:  gpd.GeoDataFrame({
    'id_parcel':  pdf['id_parcel'],
    'tile':  pdf['tile'],
    'bareground_percent_survey':  pdf['bareground_percent_survey'],
    'polygon_geometry': gpd.GeoSeries.from_wkb(pdf['geometry_polygon'], crs=27700),
    'point_geometry': gpd.GeoSeries.from_wkb(pdf['geometry'], crs=27700),
  }, crs=27700, geometry='point_geometry'))
)

# COMMAND ----------

count_dis_parcels = df['id_parcel'].nunique()
count_dis_tiles = df['tile'].nunique()
dis_tiles = df['tile'].unique()

print( f'There are {count_dis_parcels} distinct parcels in the surveys in the dataset. \n These parcels are in {count_dis_tiles} tiles around Enland. \n The following tiles: {dis_tiles}')
df.head()

# COMMAND ----------

df.loc[df['tile'] =="30UXF"].plot(color='C1')

# COMMAND ----------

df_sample = df.loc[df['tile']=='30UWC']

ax = df_sample['polygon_geometry'].plot(color='C2')
df_sample['point_geometry'].plot(ax=ax,color='C3')

# COMMAND ----------

import matplotlib.pyplot as plt
fig, axs = plt.subplots(nrows=3, ncols=3, figsize=(6,6))

for tile, ax in zip(dis_tiles, axs.ravel()):
    df_sample = df.loc[df['tile']==tile]
    
    df_sample.loc[df.tile == tile, 'polygon_geometry'].plot(ax=ax, color='C2')
    df_sample.loc[df.tile == tile, 'point_geometry'].plot(ax=ax, color='C3')
    
fig.show()

# COMMAND ----------

'''
To do:
    function
    given a point, locate point in tiff
    add da to dataframe
    
stats.models:;
    regplot from seaborns
    regression model
'''

# COMMAND ----------

###Creating function to add NDVI to specific points to a given dataset
def add_ndvi_to_gdf_by_point(df):
    """
    Parameters:
        df: GeoDataFrame - dataframe to add the rioxarray data into
    Returns:
        df: GeoDataframe 
    """
    list_of_tiles = df['tile'].unique()

    def point_clip(point, arr):
        value = arr.sel(x=point.x, y=point.y, method="nearest").values.item()
        return value

        #df['point_geometry'] = df['point_geometry'].to_crs(epsg=da.rio.crs.to_epsg())
    for tile in list_of_tiles:
        path_ndvi = f"/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/ndvi/T{tile}-{month_fm}-survey.tif"
        da = rxr.open_rasterio(path_ndvi).squeeze(drop=True)    

        df.loc[df.tile == tile, 'NDVI']  = (df
                                            .loc[df.tile == tile,'point_geometry']
                                            .to_crs(epsg=da.rio.crs.to_epsg())
                                            .map(lambda x: point_clip(x, da))
                                            )

    df['point_geometry'] = df['point_geometry'].to_crs(epsg='27700')
    return df
    
add_ndvi_to_gdf_by_point(df)

# COMMAND ----------


