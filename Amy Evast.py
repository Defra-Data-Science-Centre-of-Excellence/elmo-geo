# Databricks notebook source
# MAGIC %md
# MAGIC # EVAST geospatial parcel features
# MAGIC provide evast with hedge+ features for parcels, not geospatial  
# MAGIC remove polygon from buffer  
# MAGIC
# MAGIC
# MAGIC ### Impact
# MAGIC This will feed directly into EVAST modelling so we will better understand impact of hedges, waterbody actions and possibly peatland restoration with batter baseline data.
# MAGIC I 
# MAGIC
# MAGIC
# MAGIC ### Data
# MAGIC - rpa-parcels  (~~[DQ]()~~)
# MAGIC - rpa-hedge  (~~[DQ]()~~)
# MAGIC - os-ngd  (~~[DQ]()~~)
# MAGIC
# MAGIC
# MAGIC ### Outputs
# MAGIC - elmo_geo-hedge_sjoin:  id_hedge, id_parcel, geometry  (~~[DQ]()~~)
# MAGIC - elmo_geo-water_sjoin:  osid, id_parcel, geometry  (~~[DQ]()~~)
# MAGIC - elmo_geo-ditch_sjoin:  osid, id_parcel, geometry  (~~[DQ]()~~)
# MAGIC - elmo_geo-unified_buffer_features
# MAGIC     - water:  id_parcel, m, ha (buf: 0, 4, 8, 12)
# MAGIC     - ditch:  id_parcel, m, ha (buf: 0, 4, 8, 12)
# MAGIC     - ~~sylvan_~~hedge:  id_parcel, m, ha (buf: 0, 4, 8, 12)
# MAGIC     - ~~sylvan_wood~~:  
# MAGIC     - ~~sylvan_relict~~:  
# MAGIC     - ~~wall~~:  
# MAGIC     - ~~available~~:  
# MAGIC
# MAGIC *elmo_geo-water_sjoin is 448GB, I rethink saving sjoin*
# MAGIC
# MAGIC
# MAGIC ### QA
# MAGIC | ☐/🗹 | Task | Person | Date | Notes |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | 🗹   | Author | Andrew West | 2023-12-04 |
# MAGIC | ☐   | Impact | Andrew West | 2023-12-04 | Low |
# MAGIC | ☐   | Data | Andrew West | 2023-12-04 |
# MAGIC | ☐   | QA |
# MAGIC
# MAGIC ### RACI
# MAGIC Responsible, Accountable, Consulted, Informed
# MAGIC
# MAGIC

# COMMAND ----------

import os
from glob import glob
import pandas as pd
import geopandas as gpd
from pyspark.sql import functions as F, types as T

import elmo_geo
from elmo_geo.io import to_gpq, load_sdf, download_link_file, download_link_dir
from elmo_geo.io.file import st_simplify
from elmo_geo.st import sjoin
from elmo_geo.utils.misc import gtypes  # debug

elmo_geo.register()

# COMMAND ----------

tile = 'TL49'

sdf_parcels = spark.read.format('geoparquet').load(f'dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcels-adas.parquet/sindex={tile}*')
sdf_hedges = spark.read.format('geoparquet').load(f'dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-adas.parquet/sindex={tile}*')
sdf_hedges_2023 = spark.read.format('geoparquet').load(f'dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-2023_11_13.parquet/sindex={tile}*')

print(sdf_parcels.count(), sdf_hedges.count(), sdf_hedges_2023.count())

calc_parcels_with_hedges = lambda n: sjoin(sdf_parcels, sdf_hedges, how='inner', distance=n).select('id_parcel').distinct().count()

df = pd.DataFrame({'spatial_join_distance': range(0, 200, 4)})
df['parcels_with_hedges'] = df['spatial_join_distance'].apply(calc_parcels_with_hedges)

df.plot(x='spatial_join_distance', y='parcels_with_hedges')


sf = 'dbfs:/mnt/lab/restricted/ELM-Project/ods/{}.parquet/sindex={}'
f = '/dbfs/mnt/lab/restricted/ELM-Project/ods/{}.parquet/sindex={}'

sdf_parcel = spark.read.format('geoparquet').load(sf.format('rpa-parcels-adas', tile)).select('id_parcel', 'geometry')
sdf_hedge = spark.read.format('geoparquet').load(sf.format('rpa-hedge-2023_11_13', tile)).select('geometry')
gdf_parcel = gpd.read_parquet(f.format('rpa-parcels-adas', tile))
gdf_hedge = gpd.read_parquet(f.format('rpa-hedge-2023_11_13', tile))


sdf = sjoin(sdf_parcel, sdf_hedge, rsuffix='', how='inner', distance=12)
gdf = sdf.toPandas()


a, b = gdf['id_parcel'].nunique(), gdf_parcel['id_parcel'].nunique()
print(a, b, a/b)

ax = gpd.GeoSeries(gdf['geometry_left']).plot(figsize=(16,16), color='darkgoldenrod')
ax = gdf_parcel.plot(ax=ax, color='darkgoldenrod', alpha=.3, edgecolor='k', linewidth=.5)
ax = gdf_hedge.plot(ax=ax, color='r')
ax = gpd.GeoSeries(gdf['geometry']).plot(ax=ax, color='g')
ax.axis('off')

# COMMAND ----------

def st_valid(col:str='geometry'):
    null = 'ST_GeomFromText("Point EMPTY")'
    expr = f'COALESCE(ST_MakeValid({col}), {null})'
    expr = f'{expr} AS {col}'
    return F.expr(expr)

def st_to_polygon(col:str='geometry', precision:float=3) -> F.expr:
    '''Convert Points and LineStrings to Polygons
    This is required for the union of geometries of different dimentionality
    '''
    return F.expr(f'ST_Buffer({expr}, {10**-precision}) AS {col}')

def fn_m(name):
    '''Calculate the length of overlap between the named feature and parcel
    '''
    null = 'ST_GeomFromText("Point EMPTY")'
    expr = f'COALESCE(ST_MakeValid(geometry_{name}), {null})'
    expr = f'ST_MakeValid(ST_Intersection(geometry, {expr}))'
    expr = f'ST_Length({expr})'
    expr = f'{expr} AS m_{name}'
    return F.expr(expr)

def fn_ha(name, buf):
    '''Calculate the buffered area overlap between the named feature and parcel
    '''
    null = 'ST_GeomFromText("Point EMPTY")'
    expr = f'COALESCE(ST_MakeValid(geometry_{name}), {null})'
    expr = f'ST_MakeValid(ST_Buffer({expr}, {buf}))' if buf else expr
    expr = f'ST_MakeValid(ST_Intersection(geometry, {expr}))'
    expr = f'ST_Area({expr})'
    expr = f'{expr} AS ha_{name}_buf{buf}m' if buf else f'{expr} AS ha_{name}'
    return F.expr(expr)


# COMMAND ----------

# MAGIC %ls /dbfs/mnt/lab/restricted/ELM-Project/ods/

# COMMAND ----------

# gsjoin
mode = 'ignore'

distance = 12
drains = ['Ditch', 'Named Ditch', 'Moat']
sdf_parcels = load_sdf('rpa-parcels-adas').select('id_parcel', 'geometry')
sdf_hedges = load_sdf('rpa-hedge-adas').select('geometry')
sdf_hedges_new = load_sdf('rpa-hedge-2023_11_13').select('id_parcel', 'geometry')
sdf_water = load_sdf('os-water-2022').filter(~F.col('description').isin(drains)).select('geometry')
sdf_ditchs = load_sdf('os-water-2022').filter(F.col('description').isin(drains)).select('geometry')
sdf_walls = load_sdf('os-wall-2022').select('geometry')


def add_extra_hedges(sdf, check):
    if check:
        sdf = sdf.unionByName(
            sdf_hedges_new.join(sdf.select('id_parcel'), on='id_parcel', how='inner'),
            allowMissingColumns = True,
        )
    return sdf


for name, sdf_other in {
    'hedge': sdf_hedges,
    # 'water': sdf_water,
    # 'ditch': sdf_ditchs,
    # 'wall': sdf_walls,
}.items():
    print(name, sdf_parcels.columns, sdf_other.columns)
    sdf = (
        sjoin(sdf_parcels, sdf_other, rsuffix='', how='left', distance=12)
        # .select('id_parcel', 'geometry').join(sdf_parcels.select('id_parcel'), on='id_parcel', how='right')  # left join bug
        # .transform(add_extra_hedges, name=='hedge')
        .withColumn('geometry', F.expr('ST_MakeValid(ST_Buffer(geometry, 0.001))'))  # to polygon
        # .withColumn('geometry', F.expr('EXPLODE(ST_Dump(geometry))'))
        # .withColumn('gtype', F.expr('ST_GeometryType(geometry)'))
        # .groupby('id_parcel', 'gtype').agg(F.expr('ST_Union_Aggr(geometry) AS geometry'))
        .groupby('id_parcel').agg(F.expr('ST_Union_Aggr(geometry) AS geometry'))
        .withColumn('geometry', st_simplify())
    )
    print(f'{name}\tParcels: {sdf_parcels.count():,}    Other: {sdf_other.count():,}   Out: {sdf.count():,}')
    sf = f'dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-{name}_sjoin-2023_12_12.parquet'
    to_gpq(sdf, sf, mode=mode)


# COMMAND ----------

sdf_parcels = load_sdf('rpa-parcels-adas').drop('sindex')
sdf_hedges = load_sdf('elmo-hedge-2023_12_05').drop('sindex')
sdf_water = load_sdf('elmo-water-2023_12_05').drop('sindex')
sdf_ditchs = load_sdf('elmo-ditch-2023_12_05').drop('sindex')
sdf_walls = load_sdf('elmo-wall-2023_12_05').drop('sindex')
names = 'hedge', 'water', 'ditch', 'wall'
bufs = 0, 4, 8, 12


null = 'ST_GeomFromText("Point EMPTY")'
sdf = (sdf_parcels
    .join(sdf_hedges.withColumnRenamed('geometry', 'geometry_hedge'), on='id_parcel', how='left').withColumn('geometry_hedge', F.expr(f'COALESCE(geometry_hedge, {null})'))
    .join(sdf_water.withColumnRenamed('geometry', 'geometry_water'), on='id_parcel', how='left').withColumn('geometry_water', F.expr(f'COALESCE(geometry_water, {null})'))
    .join(sdf_ditchs.withColumnRenamed('geometry', 'geometry_ditch'), on='id_parcel', how='left').withColumn('geometry_ditch', F.expr(f'COALESCE(geometry_ditch, {null})'))
    .join(sdf_walls.withColumnRenamed('geometry', 'geometry_wall'), on='id_parcel', how='left').withColumn('geometry_wall', F.expr(f'COALESCE(geometry_wall, {null})'))
    .select(
        'id_parcel',
        *(F.expr(f'ST_Length(ST_Intersection(geometry, geometry_{name})) AS m_{name}') for name in names),
        *(F.expr(f'ST_Area(ST_Intersection(geometry, ST_Buffer(geometry_{name}, {buf})))/10000 AS ha_{name}_buf{buf}m') for name in names for buf in bufs),
    )
)
df = sdf.toPandas()
df.to_feather('/dbfs/mnt/lab/restricted/ELM-Project/out/elmo-buffers-2023_12_05.feather')

df

# COMMAND ----------

displayHTML('<br>'.join([
    # download_link_dir('/dbfs/mnt/lab/restricted/ELM-Project/ods/elmo-hedge-2023_12_05.parquet'),
    # download_link_dir('/dbfs/mnt/lab/restricted/ELM-Project/ods/elmo-water-2023_12_05.parquet'),
    # download_link_dir('/dbfs/mnt/lab/restricted/ELM-Project/ods/elmo-ditch-2023_12_05.parquet'),
    # download_link_dir('/dbfs/mnt/lab/restricted/ELM-Project/ods/elmo-wall-2023_12_05.parquet'),
    download_link_file('/dbfs/mnt/lab/restricted/ELM-Project/ods/elmo-buffer-2023_12_05.feather'),
]))

# COMMAND ----------

sdf_parcel = load_sdf('rpa-parcels-adas')
sdf_hedge = load_sdf('elmo-hedge')
sdf_water = load_sdf('elmo-water')
sdf_ditch = load_sdf('elmo-ditch')
names = 'hedge', 'water', 'ditch'
bufs = 0, 4, 8, 12


def fn_m(name):
    '''Calculate the length of overlap between the named feature and parcel
    '''
    null = 'ST_GeomFromText("Point EMPTY")'
    expr = f'COALESCE(geometry_{name}, {null})'
    expr = f'ST_MakeValid(ST_Intersection(geometry, {expr}))'
    expr = f'ST_Length({expr})'
    expr = f'{expr} AS m_{name}'
    return F.expr(expr)


def fn_ha(name, buf):
    '''Calculate the buffered area overlap between the named feature and parcel
    '''
    null = 'ST_GeomFromText("Point EMPTY")'
    expr = f'COALESCE(geometry_{name}, {null})'
    expr = f'ST_MakeValid(ST_Buffer({expr}, {buf}))' if buf else expr
    expr = f'ST_MakeValid(ST_Intersection(geometry, {expr}))'
    expr = f'ST_Area({expr})'
    expr = f'{expr} AS ha_{name}_buf{buf}m' if buf else f'{expr} AS ha_{name}'
    return F.expr(expr)


sdf_out = (sdf_parcel
    .join(sdf_hedge.withColumnRenamed('geometry', 'geometry_hedge'), on='id_parcel', how='left')
    .join(sdf_water.withColumnRenamed('geometry', 'geometry_water'), on='id_parcel', how='left')
    .join(sdf_ditch.withColumnRenamed('geometry', 'geometry_ditch'), on='id_parcel', how='left')
    .select(
        'id_parcel',
        *(fn_m(name) for name in names),
        *(fn_ha(name, buf) for name in names for buf in bufs),
    )
)


sdf_out.filter('0<m_hedge').show()
df = sdf_out.toPandas()
df.to_feather('/dbfs/mnt/lab/restricted/ELM-Project/out/parcel_features.feather')
display(df)
df.sum()
