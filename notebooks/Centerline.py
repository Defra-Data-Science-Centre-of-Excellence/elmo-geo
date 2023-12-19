# Databricks notebook source
# MAGIC %pip install -q centerline

# COMMAND ----------

import geopandas as gpd
from shapely import affinity, Polygon
from centerline.geometry import Centerline

df = gpd.GeoDataFrame(geometry=[
  affinity.rotate(Polygon([(0, 0), (0, 4), (1, 4), (1, 0)]), 30),
  Polygon([[0, 0], [0, 4], [4, 4], [4, 0]]),
])
df['centerline'] = df.geometry.apply(lambda g: Centerline(g).geometry)


df['centerline'].plot(ax=df.plot(alpha=.5), color='k')
df

# COMMAND ----------

df2 = gpd.read_parquet('/dbfs/mnt/lab/restricted/ELM-Project/ods/elmo_geo-hedge_sjoin-2023_12_12.parquet/sindex=NY97').set_crs(epsg=27700).explode().reset_index(drop=True)
df2

# COMMAND ----------

a = 14
df4 = df2.assign(l=df2.length).sort_values('l').iloc[a:a+1]
df4.geometry = df4.geometry.buffer(1)
df4['centerline'] = df4.geometry.apply(lambda g: Centerline(g).geometry)
# df4['centerline2'] = df4['centerline'].apply(lambda g: Centerline(g.buffer(.5)).geometry)
df4['centerline'].plot(ax=df4.plot(alpha=.5), color='k')
df4

# COMMAND ----------

import geopandas as gpd
from pyspark.sql import functions as F

import elmo_geo
from elmo_geo.st import sjoin

elmo_geo.register()

# COMMAND ----------

sdf_pl = spark.read.format('geoparquet').load('dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-protected_landscapes-2023_12_12.parquet')
sdf_parcel = spark.read.format('geoparquet').load('dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcels-adas.parquet/sindex=NY76*')
sdf_hedge = spark.read.format('geoparquet').load('dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-adas.parquet/sindex=NY76*')

sdf_pl.createOrReplaceTempView('left')
sdf_hedge.createOrReplaceTempView('right')


sdf = spark.sql('''
SELECT
    l.type,
    l.name,
    r.gtype,
    ST_Union_Aggr(r.geometry) AS geometry
FROM
    (SELECT `PL Type` AS type, `PL Name` AS name, geometry FROM left) l
LEFT JOIN
    (SELECT ST_GeometryType(geometry) AS gtype, geometry FROM right) r
ON
    ST_Intersects(COALESCE(l.geometry, ST_GeomFromText("Point EMPTY")), COALESCE(r.geometry, ST_GeomFromText("Point EMPTY")))
GROUP BY
    l.type,
    l.name,
    r.gtype
''')


display(sdf)

# COMMAND ----------

sdf_parcel = spark.read.format('geoparquet').load('dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcels-adas.parquet/sindex=NY76*')
sdf_hedge = spark.read.format('geoparquet').load('dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-adas.parquet/sindex=NY76*')

sdf_parcel.createOrReplaceTempView('left')
sdf_hedge.createOrReplaceTempView('right')


# # basic spatial join
# sdf = spark.sql('''
# SELECT *
# FROM left l, right r
# WHERE ST_Intersects(l.geometry, r.geometry)
# ''')


# groupby spatial distance join
## gtype, due to lack of GeometryCollection support
## sindex, is created during ingestion
null = 'ST_GeomFromText("Point EMPTY")'
sdf = spark.sql(f'''
SELECT
    l.id_parcel,
    r.gtype,
    ST_Union_Aggr(r.geometry) AS geometry
FROM (SELECT id_parcel, geometry, sindex FROM left) l
LEFT JOIN (SELECT ST_GeometryType(geometry) AS gtype, geometry, sindex FROM right) r
ON sindex AND (ST_Intersects(COALESCE(l.geometry, {null}), COALESCE(r.geometry, {null})) OR r.geometry IS NULL)
GROUP BY
    l.id_parcel,
    r.gtype
''')


# COMMAND ----------

import matplotlib.pyplot as plt
import geopandas as gpd

gdf0 = gpd.read_parquet('/dbfs/mnt/lab/restricted/ELM-Project/ods/rpa-parcels-adas.parquet/sindex=NY76')
gdf1 = gpd.read_parquet('/dbfs/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-adas.parquet/sindex=NY76')
gdf2 = gpd.read_parquet('/dbfs/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-2023_11_13.parquet/sindex=NY76')

fig, ax = plt.subplots(1, 1, figsize=(16,16))
gdf0.plot(ax=ax, linewidth=.5, color='darkgoldenrod', alpha=.3, edgecolor='darkgoldenrod', label='parcel')
gdf1.plot(ax=ax, linewidth=1, color='r', label='managed hedge')
gdf2.plot(ax=ax, linewidth=1, color='g', label='control hedge')
ax.axis('off')
ax.legend()
