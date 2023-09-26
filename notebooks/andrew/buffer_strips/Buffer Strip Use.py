# Databricks notebook source
# MAGIC %pip install -qU git+https://github.com/aw-west-defra/cdap_geo.git

# COMMAND ----------

# DBTITLE 1,Import
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
import geopandas as gpd

from cdap_geo.sedona import st_register, F, st
st_register()

today = datetime.now().isoformat().split('T')[0]

# COMMAND ----------

# DBTITLE 1,Filepaths
sf_geo = 'dbfs:/mnt/lab/unrestricted/DSMT/gis/buffer_strips_geometries/2023-01-19.parquet'

sf = f'dbfs:/mnt/lab/unrestricted/DSMT/gis/not_buffer_strips/{today}.parquet'

# COMMAND ----------

# DBTITLE 1,Not Geometries
buf = 4
sdf0 = (spark.read.parquet(sf_geo)
  .withColumn('geometry_p', st('ST_Boundary(geometry_parcel)'))
  .withColumn('geometry_h', st(f'ST_Buffer(geometry_hedge, {buf})'))
  .withColumn('geometry_w', st(f'ST_Buffer(geometry_water, {buf})'))
  .withColumn('geometry_u', st('ST_Union(geometry_h, geometry_w)'))
  .withColumn('geometry_1', st('ST_Difference(geometry_p, geometry_h)', True))
  .withColumn('geometry_2', st('ST_Difference(geometry_p, geometry_w)', True))
  .withColumn('geometry_3', st('ST_Difference(geometry_p, geometry_u)', True))
  .select(
    'id_business',
    'id_parcel',
    'geometry_parcel', 'geometry_hedge', 'geometry_water',
    'geometry_1', 'geometry_2', 'geometry_3',
  )
)

display(sdf0)

# COMMAND ----------

# DBTITLE 1,Plot
def wkbs(data, crs):
  return gpd.GeoSeries.from_wkb(data, crs=crs)

def sql_startswiths(col:str, starters:list):
  return ' OR '.join(f'STARTSWITH({col}, "{s}")' for s in starters)


gdf = (sdf0
  .filter(sql_startswiths('id_parcel', ['NY9170', 'NY9171', 'NY9270', 'NY9271']))
  .select(
    'id_business',
    'id_parcel',
    F.expr('ST_AsBinary(geometry_parcel)').alias('geometry_parcel'),
    F.expr('ST_AsBinary(geometry_hedge)').alias('geometry_hedge'),
    F.expr('ST_AsBinary(geometry_water)').alias('geometry_water'),
    F.expr('ST_AsBinary(geometry_1)').alias('geometry_1'),
    F.expr('ST_AsBinary(geometry_2)').alias('geometry_2'),
    F.expr('ST_AsBinary(geometry_3)').alias('geometry_3'),
  )
  .toPandas()
  .assign(
    geometry_parcel = lambda df: wkbs(df['geometry_parcel'], crs=27700),
    geometry_hedge = lambda df: wkbs(df['geometry_hedge'], crs=27700),
    geometry_water = lambda df: wkbs(df['geometry_water'], crs=27700),
    geometry_1 = lambda df: wkbs(df['geometry_1'], crs=27700),
    geometry_2 = lambda df: wkbs(df['geometry_2'], crs=27700),
    geometry_3 = lambda df: wkbs(df['geometry_3'], crs=27700),
  )
  .pipe(gpd.GeoDataFrame)
)


fig, ((ax0, ax1), (ax2, ax3)) = plt.subplots(2,2, figsize=(9,9))
gdf['geometry_parcel'].plot(ax=ax0, color='darkgoldenrod', alpha=.6, edgecolor='darkgoldenrod').set(title='BNG: NY9170-NY9271')
gdf['geometry_hedge'].plot(ax=ax0, color='C2', linewidth=1)
gdf['geometry_water'].plot(ax=ax0, color='C0', linewidth=1)

gdf['geometry_parcel'].boundary.plot(ax=ax1, color='darkgoldenrod', linewidth=.2).set(title='Available Boundaries')
gdf['geometry_3'].explode()[lambda g: g.type!='Point'].plot(ax=ax1, color='darkgoldenrod', linewidth=1, label='Parcel')

gdf['geometry_hedge'].plot(ax=ax2, color='C2', linewidth=1, label='Hedgerow').set(title='Hedgerows')
gdf['geometry_1'].plot(ax=ax2, color='darkgoldenrod', linewidth=.2)

gdf['geometry_water'].plot(ax=ax3, color='C0', linewidth=1, label='Waterbody').set(title='Waterbodies')
gdf['geometry_2'].plot(ax=ax3, color='darkgoldenrod', linewidth=.2)

ax0.axis('off'); ax1.axis('off'); ax2.axis('off'); ax3.axis('off'); fig.tight_layout(); fig.legend(loc='center', framealpha=0)

# COMMAND ----------

# DBTITLE 1,Metrics
sdf1 = sdf0.select(
  'id_business',
  'id_parcel',
  F.expr('ST_Length(geometry_parcel)').alias('m_parcel'),
  F.expr('ST_Length(geometry_hedge)').alias('m_hedge'),
  F.expr('ST_Length(geometry_water)').alias('m_water'),
  F.expr('ST_Length(geometry_1)').alias('m_not_hedge'),
  F.expr('ST_Length(geometry_2)').alias('m_not_water'),
  F.expr('ST_Length(geometry_3)').alias('m_not_either'),
)

sdf1.write.parquet(sf, mode='overwrite')
display(sdf1)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Statistics
from pprint import pprint

df = spark.read.parquet(sf).groupBy().sum().toPandas()
p, h, w = df['sum(m_parcel)'].sum(), df['sum(m_hedge)'].sum(), df['sum(m_water)'].sum()
n1, n2, n3 = df['sum(m_not_hedge)'].sum(), df['sum(m_not_water)'].sum(), df['sum(m_not_either)'].sum()

pprint({
  'Margins': f'{p:,.0f} m',
  'Hedgerow': f'{h:,.0f} m',
  'Waterbody': f'{w:,.0f} m',
  'Margin not Hedgerow': (f'{n1:,.0f} m', f'{n1/p:.0%}'),
  'Margin not Waterbody': (f'{n2:,.0f} m', f'{n2/p:.0%}'),
  'Margin not Either': (f'{n3:,.0f} m', f'{n3/p:.0%}'),
})

# COMMAND ----------

# DBTITLE 1,Download
df = spark.read.parquet(sf)

display(df)

# COMMAND ----------

pan_df = df.toPandas()

# COMMAND ----------


