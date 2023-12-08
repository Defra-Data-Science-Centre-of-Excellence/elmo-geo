# Databricks notebook source
# MAGIC %sh
# MAGIC du -sh /dbfs/mnt/lab/restricted/ELM-Project/*/*

# COMMAND ----------

# MAGIC %sh
# MAGIC du -sh /dbfs/mnt/lab/unrestricted/elm/*
# MAGIC du -sh /dbfs/mnt/lab/unrestricted/elm_data/*
# MAGIC du -sh /dbfs/mnt/lab/unrestricted/elm/buffer_strips/*

# COMMAND ----------

# MAGIC %sh
# MAGIC du -sh /dbfs/mnt/base/unrestricted/source_rpa*/dataset_*

# COMMAND ----------

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
