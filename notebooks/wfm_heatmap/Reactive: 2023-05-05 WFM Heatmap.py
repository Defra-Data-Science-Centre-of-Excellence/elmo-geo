# Databricks notebook source
from pyspark.sql import functions as F, types as T
from sedona.register import SedonaRegistrator
SedonaRegistrator.registerAll(spark)

import pandas as pd
import geopandas as gpd


def pdf2gdf(sdf, crs=27700):
  return gpd.GeoDataFrame(
    pdf,
    geometry = gpd.GeoSeries.from_wkb(pdf['geometry'], crs=crs),
    crs = crs,
  )

def sdf2gdf(sdf, crs=27700):
  return (sdf
    .withColumn('geometry', F.expr('ST_AsBinary(geometry)'))
    .toPandas()
    .pipe(pdf2gdf)
  )


# COMMAND ----------

sf_parcel = 'dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet/'
sf_wfm = 'dbfs:/mnt/lab/unrestricted/elm/wfm/v3.parquet'

f_out = {
  100_000: '/dbfs/mnt/lab/unrestricted/elm/buffer_strips/100km_wfm.parquet',
  10_000: '/dbfs/mnt/lab/unrestricted/elm/buffer_strips/10km_wfm.parquet',
  1_000: '/dbfs/mnt/lab/unrestricted/elm/buffer_strips/1km_wfm.parquet',
  100: '/dbfs/mnt/lab/unrestricted/elm/buffer_strips/ha_wfm.parquet',
  'parcel': '/dbfs/mnt/lab/unrestricted/elm/buffer_strips/parcel_wfm.parquet',
}

sf_out = {
  100_000: 'dbfs:/mnt/lab/unrestricted/elm/buffer_strips/100km_wfm.parquet',
  10_000: 'dbfs:/mnt/lab/unrestricted/elm/buffer_strips/10km_wfm.parquet',
  1_000: 'dbfs:/mnt/lab/unrestricted/elm/buffer_strips/1km_wfm.parquet',
  100: 'dbfs:/mnt/lab/unrestricted/elm/buffer_strips/ha_wfm.parquet',
  'parcel': 'dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcel_wfm.parquet',
}

# COMMAND ----------

df_wfm.columns

# COMMAND ----------

res = 'parcel'

# Denormalise WFM
df_wfm = spark.read.parquet(sf_wfm)
for col in df_wfm.columns:
  if any(col.startswith(x) for x in ['gmpn_', 'gmplu_']):
    df_wfm = df_wfm.drop(col)
  elif col.startswith('tph_'):
    col_new = col.replace('tph_', 't_')
    col_ha = col.replace('tph_', 'ha_')
    df_wfm = (df_wfm
      .withColumn(col_new, F.expr(f'{col}*{col_ha}'))
      .drop(col)
    )
  elif col.startswith('gmph_'):
    col_new = col.replace('gmph_', 'gm_')
    col_ha = col.replace('gmph_', 'ha_')
    df_wfm = (df_wfm
      .withColumn(col_new, F.expr(f'{col}*{col_ha}'))
      .drop(col)
    )

# Centroid
df_parcel = (spark.read.parquet(sf_parcel)
  .withColumn('centroid', F.expr('ST_Centroid(geometry)'))
  .withColumn('centroid4326', F.expr('ST_FlipCoordinates(ST_Transform(centroid, "EPSG:27700", "EPSG:4326"))'))
  .select(
    'id_parcel',
    F.expr('ST_X(centroid) AS x'),
    F.expr('ST_Y(centroid) AS y'),
    F.expr('ST_X(centroid4326) AS lng'),
    F.expr('ST_Y(centroid4326) AS lat'),
  )
)

# Join
df = df_parcel.join(
  df_wfm,
  on = 'id_parcel',
).drop(
  'id_business',
  'id_parcel',
  'arable_land_classification',
  '__index_level_0__',
).na.drop(
  subset = ['x', 'y']
)

# Output
dbutils.fs.rm(sf_out[res], recurse=True)
df.toPandas().to_parquet(f_out[res])
display(df)

# COMMAND ----------

for res in [100_000, 10_000, 1000, 100]:
  print(res)

  # Gridify
  df = spark.read.parquet(sf_out['parcel'])
  df = (df
    .withColumn('x', F.expr(f'FLOOR(x/{res})*{res}'))
    .withColumn('y', F.expr(f'FLOOR(y/{res})*{res}'))
    .withColumn('count', F.lit(1))
    .groupby('x', 'y').agg(
      *[F.expr(f'NVL(SUM(`{col}`), 0) AS `{col}`')
      for col in ['count', *df.columns]
      if col not in ['x', 'y']
    ])
    .withColumn('geometry', F.expr(f'ST_PolygonFromEnvelope(x, y, x+{res}, y+{res})'))
    .withColumn('geometry', F.expr('ST_FlipCoordinates(ST_Transform(geometry, "EPSG:27700", "EPSG:4326"))'))
    .withColumn('lng', F.expr('ST_X(ST_Centroid(geometry))'))
    .withColumn('lat', F.expr('ST_Y(ST_Centroid(geometry))'))
  )

  # Output
  dbutils.fs.rm(sf_out[res], recurse=True)
  if res in [1000, 100]:
    df.drop('geometry').toPandas().to_parquet(f_out[res])
  else:
    df.toPandas().pipe(gpd.GeoDataFrame).set_crs(4326).to_parquet(f_out[res])
  display(df)
