# Databricks notebook source
# MAGIC %md # Bare Soil Survey Data Cleaning
# MAGIC We have received survey data so we can evaluate
# MAGIC model analysis. We would like to see whether
# MAGIC the satellite data lines up with the survey data that we have got.
# MAGIC
# MAGIC In this notwbook, we will:
# MAGIC - Locate data required from 3 sources:
# MAGIC Parcel geometries, point of survey and NDVI calculated for point
# MAGIC - overlay points to see which parcels have survey points located inside
# MAGIC - Find NDVI calculated for each point

# COMMAND ----------

import os
import geopandas as gpd
from pyspark.sql import functions as F
from xarray.core.dataarray import DataArray
import rioxarray as rxr
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


files = [f for f in os.listdir(path_surveys) if f.startswith("SFI_") and f.endswith(".csv")]
print(f"Found {len(files)} matching files")

# COMMAND ----------

files_paths = [
    f"{path_surveys.replace('/dbfs','')}/{f}"  for f in files]
files_iter = iter(files_paths)
df = spark.read.csv(next(files_iter), header=True, inferSchema=True).select(
                'Farm-FieldRLR_Number',
                'Groundcover-QuadratLocation-Latitude',
                'Groundcover-QuadratLocation-Longitude',
                'Groundcover-BareGround'
            )
for f in files_iter:
    df = df.union(
        (
            spark.read.csv(f, header=True, inferSchema=True).select(
                'Farm-FieldRLR_Number',
                'Groundcover-QuadratLocation-Latitude',
                'Groundcover-QuadratLocation-Longitude',
                'Groundcover-BareGround'
            )
        )
    )
survey_df = (df
             .withColumnRenamed('Groundcover-QuadratLocation-Latitude', 'latitude')
             .withColumnRenamed('Groundcover-QuadratLocation-Longitude', 'longitude')
             .filter('latitude is not null AND longitude is not null')
             .select(
                    F.col('Farm-FieldRLR_Number').alias('Farm_number'),
                    (F.col('Groundcover-BareGround')/100).alias('bareground_percent_survey'),
                    F.expr(
                        'ST_Transform(ST_Point(latitude, longitude), "epsg:4326","epsg:27700")'
                    ).alias('geometry')
                )
             )

# COMMAND ----------

# getting parcels compatible for survey
parcel_df = spark.read.parquet(path_parcels)

parcel_df = (
    parcel_df
    .select('id_parcel', 'tile', 'geometry')
    .withColumn('geometry', F.expr('ST_GeomFromWKB(hex(geometry))'))
    .withColumnRenamed('geometry', 'geometry_polygon')
)
display(parcel_df)

# COMMAND ----------

# Combining parcel and survey data frames to check points inside each tile
cross_df = parcel_df.join(survey_df, how='cross')

cross_df = (
    cross_df.withColumn(
        'is_within',
        F.expr(f'ST_Within(geometry,geometry_polygon)')
    )
)
cross_df = cross_df.filter(cross_df.is_within is True)

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

print(f'There are {count_dis_parcels} distinct parcels in the surveys',
      f' in the dataset. \n These parcels are in {count_dis_tiles} tiles',
      f' around Enland. \n The following tiles: {dis_tiles}')
df.head()

# COMMAND ----------

df_sample = df.loc[df['tile'] == '30UWC']

ax = df_sample['polygon_geometry'].plot(color='C2')
df_sample['point_geometry'].plot(ax=ax, color='C3')

# COMMAND ----------

# Creating function to add NDVI to specific points to a given dataset


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

    for tile in list_of_tiles:
        path_ndvi = f"/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/ndvi/T{tile}-{month_fm}-survey.tif"
        da = rxr.open_rasterio(path_ndvi).squeeze(drop=True)

        df.loc[df.tile == tile, 'NDVI'] = (df
                                           .loc[df.tile == tile, 'point_geometry']
                                           .to_crs(epsg=da.rio.crs.to_epsg())
                                           .map(lambda x: point_clip(x, da))
                                           )

    df['point_geometry'] = df['point_geometry'].to_crs(epsg='27700')
    return df


df = add_ndvi_to_gdf_by_point(df)

# COMMAND ----------

# Quick checks to see if data is there
print(df.columns)
na_NDVI_df = df.loc[df['NDVI'].isna() is True]
print(na_NDVI_df.shape)

print(f'There are {na_NDVI_df.shape[0]} NA values in the ',
      'dataframe. This is likely due to the cloud cover from the NDVI files.'
      )
na_NDVI_df


# COMMAND ----------

# Saving dataframe - so I can point analysis notebook to the cleaned data
