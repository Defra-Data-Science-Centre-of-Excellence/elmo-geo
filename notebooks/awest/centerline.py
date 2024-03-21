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
