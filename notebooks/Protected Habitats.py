# Databricks notebook source
from pyspark.sql import functions as F

import elmo_geo
from elmo_geo.utils.types import SparkDataFrame
from elmo_geo.io import load_sdf, ingest_esri
from elmo_geo.st import sjoin

elmo_geo.register()

# COMMAND ----------

ingest_esri('https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services/National_Parks_England/FeatureServer/0')
ingest_esri('https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services/Areas_of_Outstanding_Natural_Beauty_England/FeatureServer/0')
ingest_esri(url='https://services1.arcgis.com/ESMARspQHYMw9BZ9/ArcGIS/rest/services/Countries_December_2022_UK_BFC/FeatureServer/0')

# COMMAND ----------

sdf = load_sdf('rpa-parcels-adas')
sdf_hedge = load_sdf('hedge')
sdf_np = load_sdf('National_Parks')
sdf_aonb = load_sdf('Areas_of_Outstanding_Natural_Beauty')


sdf_pl = SparkDataFrame.union(
    sdf_np.select(
        F.lit('National Park').alias('group'),
        F.col('NAME').alias('name'),
        F.expr('ST_Transform(ST_FlipCoordinates(geometry), "epsg:4326", "epsg:27700") AS geometry'),
    ),
    sdf_aonb.select(
        F.lit('Area of Outstanding Natural Beauty').alias('group'),
        F.col('NAME').alias('name'),
        F.expr('ST_Transform(ST_FlipCoordinates(geometry), "epsg:4326", "epsg:27700") AS geometry'),
    ),
)




sdf = sjoin(
    sdf_pl,
    sdf_hedge.select('geometry'),
    how = 'left',
    lsuffix = '',
    rsuffix = '_hedge',
)


sdf.show()
sdf.count()
