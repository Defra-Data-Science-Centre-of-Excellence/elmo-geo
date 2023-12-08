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
from pyspark.sql import functions as F, types as T

import elmo_geo
from elmo_geo.utils.misc import dbfs
from elmo_geo.utils.settings import FOLDER_STG
from elmo_geo.io import to_gpq, load_sdf, download_link_file, download_link_dir
from elmo_geo.st import sjoin
from elmo_geo.utils.misc import gtypes  # debug

elmo_geo.register()

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

def st_simplify(col:str='geometry', precision:float=3) -> F.expr:
    '''Simplifies geometries
    Ensuring they are valid for other processes
    And to avoid non-noded intersection errors
    '''
    null = 'ST_GeomFromText("Point EMPTY")'
    expr = f'COALESCE(ST_MakeValid({col}), {null})'
    expr = f'ST_MakeValid(ST_SimplifyPreserveTopology(ST_MakeValid(ST_PrecisionReduce({expr}, {precision})), {10**-precision}))'
    expr = f'{expr} AS {col}'
    return F.expr(expr)

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

# stg2ods
mode = 'overwrite'


sdf_rpa_parcel = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-parcels-adas.parquet')
    .select(
        F.expr('CONCAT(RLR_RW_REFERENCE_PARCELS_DEC_21_SHEET_ID, RLR_RW_REFERENCE_PARCELS_DEC_21_PARCEL_ID) AS id_parcel'),
        F.expr('ST_SetSRID(ST_GeomFromWKB(Shape), 27700) AS geometry'),
    )
    .withColumn('geometry', st_simplify('geometry', 3))
)
to_gpq(sdf_rpa_parcel, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcels-adas.parquet', mode=mode)

sdf_rpa_parcel_2023 = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-parcels-2023_11_13.parquet')
    .select(
        # id_parcel,
        F.expr('ST_SetSRID(ST_GeomFromWKB(geom), 27700) AS geometry'),
    )
    .withColumn('geometry', st_simplify('geometry', 3))
)
to_gpq(sdf_rpa_parcel_2023, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcels-2023_11_13.parquet', mode=mode)


sdf_rpa_hedge = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-hedge-adas.parquet')
    .select(
        # id_parcel,
        # adj,
        F.expr('ST_SetSRID(ST_GeomFromWKB(geom), 27700) AS geometry'),
    )
    .withColumn('geometry', st_simplify('geometry', 3))
)
to_gpq(sdf_rpa_hedge, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-adas.parquet', mode=mode)

sdf_rpa_hedge_2023 = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-hedge-2023_11_13.parquet')
    .select(
        F.expr('CONCAT(REF_PARCEL_SHEET_ID, REF_PARCEL_PARCEL_ID) AS id_parcel'),
        F.expr('ADJACENT_PARCEL_PARCEL_ID IS NOT NULL AS adj'),
        F.expr('ST_SetSRID(ST_GeomFromWKB(the_geom), 27700) AS geometry'),
    )
    .withColumn('geometry', st_simplify('geometry', 3))
)
to_gpq(sdf_rpa_hedge_2023, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-2023_11_13.parquet', mode=mode)


os_schema = T.StructType([
    T.StructField("theme", T.StringType(), True),
    T.StructField("description", T.StringType(), True),
    T.StructField("watermark", T.StringType(), True),
    T.StructField("width", T.NumericType(), True),
    T.StructField("geometry", T.BinaryType(), True),
])

sdf_os_water = (spark.read.format('parquet')
    .schema(os_schema)
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/os-ngd-2022.parquet/wtr_*')
    .select(
        'theme',
        'description',
        'watermark',
        'width',
        F.expr('ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry'),
    )
    .withColumn('geometry', F.expr('CASE WHEN (width IS NOT NULL) THEN ST_Buffer(geometry, width) ELSE geometry END'))
    .withColumn('geometry', st_simplify('geometry', 3))
)
to_gpq(sdf_os_water, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/os-water-2022.parquet', mode=mode)

sdf_os_wall = (spark.read.format('parquet')
    .schema(os_schema)
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/os-ngd-2022.parquet')
    .filter('description REGEXP " Wall"')
    .select(
        'theme',
        'description',
        F.expr('ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry'),
    )
    .withColumn('geometry', st_simplify('geometry', 3))
)
to_gpq(sdf_os_wall, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/os-wall-2022.parquet', mode=mode)


# COMMAND ----------

# MAGIC %sh
# MAGIC du -sh /dbfs/mnt/lab/restricted/ELM-Project/ods/rpa-parcels-adas.parquet
# MAGIC du -sh /dbfs/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-adas.parquet
# MAGIC du -sh /dbfs/mnt/lab/restricted/ELM-Project/ods/os-water-2022.parquet
# MAGIC du -sh /dbfs/mnt/lab/restricted/ELM-Project/ods/os-wall-2022.parquet

# COMMAND ----------

# gsjoin
mode = 'overwrite'

distance = 12
drains = ['Ditch', 'Named Ditch', 'Moat']
sdf_parcel = load_sdf('rpa-parcels-adas').select('id_parcel', 'geometry')
sdf_hedge = load_sdf('rpa-hedge-adas').select('geometry')
sdf_water = load_sdf('os-water-2022').filter(~F.col('description').isin(drains)).select('geometry')
sdf_ditch = load_sdf('os-water-2022').filter(F.col('description').isin(drains)).select('geometry')
sdf_wall = load_sdf('os-wall-2022').select('geometry')


for name, sdf_other in {
    'hedge': sdf_hedge,
    'water': sdf_water,
    'ditch': sdf_ditch,
    'wall': sdf_wall,
}.items():
    sf = f'dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo-{name}-2023_12_05.parquet'
    sdf = (
        sjoin(sdf_parcel, sdf_other, rsuffix='', how='left', distance=12)
        .withColumn('geometry', F.expr('EXPLODE(ST_Dump(geometry))'))
        .withColumn('gtype', F.expr('ST_GeometryType(geometry)'))
        .groupby('id_parcel', 'gtype').agg(F.expr('ST_Union_Aggr(geometry) AS geometry'))
        .withColumn('geometry', st_simplify())
    )
    print(f'{name}\tParcels: {sdf_parcel.count():,}    Other: {sdf_other.count():,}   Out: {sdf.count():,}')
    to_gpq(sdf, sf, mode=mode)


# COMMAND ----------

sdf_parcel = load_sdf('rpa-parcels-adas').drop('sindex')
sdf_hedge = load_sdf('elmo-hedge-2023_12_05')
sdf_water = load_sdf('elmo-water-2023_12_05')
sdf_ditch = load_sdf('elmo-ditch-2023_12_05')
sdf_wall = load_sdf('elmo-wall-2023_12_05')
names = 'hedge', 'water', 'ditch', 'wall'
bufs = 0, 4, 8, 12


def fn_union(sdf, name):
    return (sdf
        .withColumn('geometry', F.expr('ST_Buffer(geometry, 0.001)'))
        .groupby('id_parcel')
        .agg(F.expr('ST_Union_Aggr(geometry) AS geometry'))
        .withColumn('geometry', st_simplify())
        .withColumnRenamed('geometry', f'geometry_{name}')
    )


sdf = (sdf_parcel
    .join(fn_union(sdf_hedge, 'hedge'), on='id_parcel', how='full')
    .join(fn_union(sdf_water, 'water'), on='id_parcel', how='full')
    .join(fn_union(sdf_ditch, 'ditch'), on='id_parcel', how='full')
    .join(fn_union(sdf_wall, 'wall'), on='id_parcel', how='full')
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
