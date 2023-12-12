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

# COMMAND ----------

# MAGIC %sh
# MAGIC f=/dbfs/mnt/lab/restricted/ELM-Project/ods
# MAGIC rm -r $f/elmo-hedge-2023_12_05.parquet
# MAGIC # ls $f

# COMMAND ----------

# MAGIC %sh
# MAGIC du -sh /dbfs/mnt/lab/restricted/ELM-Project/out/parcel_geometries.parquet

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/lab/restricted/ELM-Project/ods/

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/lab/restricted/ELM-Project/ods/

# COMMAND ----------

import geopandas as gpd

gdf = gpd.read_parquet('/dbfs/mnt/lab/restricted/ELM-Project/ods/rpa-parcels-adas.parquet/sindex=NT60')
gdf.to_file('left.gpkg')

gdf = gpd.read_parquet('/dbfs/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-adas.parquet/sindex=NT60')
gdf.to_file('right.gpkg')

# COMMAND ----------

# MAGIC %sh
# MAGIC # l="/dbfs/mnt/lab/restricted/ELM-Project/ods/rpa-parcels-adas.parquet/sindex=NT60"
# MAGIC # r="/dbfs/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-adas.parquet/sindex=NT60"
# MAGIC # o="out.parquet"
# MAGIC l="left.gpkg"
# MAGIC r="right.gpkg"
# MAGIC o="out.gpkg"
# MAGIC g="id_parcel"
# MAGIC
# MAGIC PATH=$PATH:/databricks/miniconda/bin
# MAGIC TMPDIR=/tmp
# MAGIC PROJ_LIB=/databricks/miniconda/share/proj
# MAGIC OGR_GEOMETRY_ACCEPT_UNCLOSED_RING=NO
# MAGIC
# MAGIC ogrinfo $l
# MAGIC ogrinfo $r
# MAGIC
# MAGIC
# MAGIC sql=$(cat <<EOF
# MAGIC SELECT l.$g, ST_Collect(r.geometry) AS geometry
# MAGIC FROM $l l
# MAGIC LEFT JOIN $r r ON ST_Intersects(l.geometry, r.geometry)
# MAGIC GROUP BY l.$g
# MAGIC EOF
# MAGIC )
# MAGIC
# MAGIC ogr2ogr -sql "$sql" $o $l
# MAGIC rm $o
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC du -sh /dbfs/mnt/lab/restricted/ELM-Project/ods/*
