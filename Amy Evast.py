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
from elmo_geo.io import to_gpq, load_sdf, download_link
from elmo_geo.io.file import st_simplify
from elmo_geo.st import sjoin

elmo_geo.register()

# COMMAND ----------

# stg2ods
mode = 'ignore'


sdf_rpa_parcel = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-parcel-adas.parquet')
    .select(
        F.expr('CONCAT(RLR_RW_REFERENCE_PARCELS_DEC_21_SHEET_ID, RLR_RW_REFERENCE_PARCELS_DEC_21_PARCEL_ID) AS id_parcel'),
        F.expr('ST_SetSRID(ST_GeomFromWKB(Shape), 27700) AS geometry'),
    )
)
to_gpq(sdf_rpa_parcel, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet', mode=mode)

sdf_rpa_parcel_2023 = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-parcel-2023_12_13.parquet')
    .select(
        # id_parcel,
        F.expr('ST_SetSRID(ST_GeomFromWKB(geom), 27700) AS geometry'),
    )
)
to_gpq(sdf_rpa_parcel_2023, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-2023_12_13.parquet', mode=mode)



sdf_rpa_hedge = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-hedge-2023_12_13.parquet')
    .select(
        F.expr('CONCAT(REF_PARCEL_SHEET_ID, REF_PARCEL_PARCEL_ID) AS id_parcel'),
        F.expr('ADJACENT_PARCEL_PARCEL_ID IS NOT NULL AS adj'),
        F.expr('TO_TIMESTAMP(VALID_FROM, "yyyyMMddHHmmss") AS valid_from'),
        F.expr('TO_TIMESTAMP(VALID_TO, "yyyyMMddHHmmss") AS valid_to'),
        F.expr('ST_SetSRID(ST_GeomFromWKB(GEOM), 27700) AS geometry'),
    )
)
to_gpq(sdf_rpa_hedge, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-2023_12_13.parquet', mode=mode)


os_schema = T.StructType([
    T.StructField('theme', T.StringType(), True),
    T.StructField('description', T.StringType(), True),
    T.StructField('watermark', T.StringType(), True),
    T.StructField('width', T.DoubleType(), True),
    T.StructField('geometry', T.BinaryType(), True),
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
)
to_gpq(sdf_os_wall, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/os-wall-2022.parquet', mode=mode)


# COMMAND ----------

# gsjoin
mode = 'ignore'

drains = ['Ditch', 'Named Ditch', 'Moat']
sdf_parcel = load_sdf('rpa-parcel-adas').select('id_parcel', 'geometry')
# sdf_hedge = load_sdf('rpa-hedge').select('geometry')
sdf_water = load_sdf('os-water-2022').filter(~F.col('description').isin(drains)).select('geometry')
# sdf_ditch = load_sdf('os-water-2022').filter(F.col('description').isin(drains)).select('geometry')
# sdf_wall = load_sdf('os-wall-2022').select('geometry')


for name, sdf_right in {
    # 'hedge': sdf_hedge,
    'water': sdf_water,
    # 'ditch': sdf_ditch,
    # 'wall': sdf_wall,
}.items():
    sdf_parcel.createOrReplaceTempView('left')
    sdf_right.createOrReplaceTempView('right')
    sdf = spark.sql('''
        SELECT 
            id_parcel,
            gtype,
            ST_MakeValid(ST_SimplifyPreserveTopology(ST_MakeValid(ST_Union_Aggr(COALESCE(geometry, ST_GeomFromText('Point EMPTY')))), 0.1)) AS geometry
        FROM (
            SELECT
                l.id_parcel,
                SUBSTRING(ST_GeometryType(ST_Multi(r.geometry)), 9) AS gtype,
                ST_MakeValid(r.geometry) AS geometry
            FROM (
                SELECT  -- make valid, simplify, buffer (x2 to avoid donut bug)
                    id_parcel,
                    ST_MakeValid(ST_SimplifyPreserveTopology(ST_MakeValid(ST_Buffer(ST_MakeValid(ST_Buffer(geometry, 0.001)), 12-0.001)), 0.1)) AS geometry
                FROM left
                WHERE geometry IS NOT NULL
            ) AS l
            JOIN (SELECT ST_MakeValid(geometry) AS geometry  -- make valid, dump, subdivide, dump, simplify
                FROM (SELECT EXPLODE(ST_Dump(geometry)) AS geometry
                    FROM (SELECT ST_SubDivideExplode(geometry, 256) AS geometry
                        FROM (SELECT EXPLODE(ST_Dump(geometry)) AS geometry
                            FROM (SELECT ST_MakeValid(ST_SimplifyPreserveTopology(geometry, 0.1)) AS geometry
                                FROM right
                                WHERE geometry IS NOT NULL
            ))))) AS r
            ON ST_Intersects(l.geometry, r.geometry)
        )
        GROUP BY id_parcel, gtype
    ''')
    spark.sql('DROP TABLE left')
    spark.sql('DROP TABLE right')
    sf = f'dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-{name}-2023_12_20.parquet'
    to_gpq(sdf, sf, mode=mode)


# COMMAND ----------

f = '/dbfs/mnt/lab/restricted/ELM-Project/out/elmo-buffers-2023_12_05.feather'
sdf_parcel = load_sdf('rpa-parcel-adas').drop('sindex')
names = 'hedge', 'water', 'ditch', 'wall'
bufs = 0, 4, 8, 12


def merge_gtypes(sdf, col:str='geometry', buf:float=1):
    if 'gtype' in sdf.columns and 1 < sdf.select('gtype').distinct().count():
        sdf = (sdf
            .withColumn(col, F.expr(f'ST_MakeValid(ST_Buffer({col}, {buf}))'))
        )
    return sdf


null = 'ST_GeomFromText("Point EMPTY")'
sdf_geoms = sdf_parcel
for name in names:
    sdf_other = (
        load_sdf(f'elmo_geo-{name}_gsjoin-2023_12_12').drop('sindex')
        .transform(merge_gtypes)
        .withColumnRenamed('geometry', f'geometry_{name}')
    )
    sdf_geoms = (sdf_geoms
        .join(sdf_other, on='id_parcel', how='left')
        .withColumn(f'geometry_{name}', F.expr(f'COALESCE(geometry_{name}, {null})'))
    )
display(sdf_geoms)

sdf = (sdf_geoms
    .select(
        'id_parcel',
        *(
            F.expr(f'ST_Length(ST_MakeValid(ST_Intersection(geometry, geometry_{name}))) AS m_{name}')
            for name in names
        ),
        *(
            F.expr(f'ST_Area(ST_MakeValid(ST_Intersection(geometry, ST_MakeValid(ST_Buffer(geometry_{name}, {buf}))))) / 10000 AS ha_{name}_buf{buf}m')
            for name in names
            for buf in bufs
        ),
    )
)


df = sdf.toPandas()
df.to_feather(f)
download_link(f)
df

# COMMAND ----------

col = 'hedge'
{
    'parcels': f"{df['id_parcel'].nunique():,}",
    f'parcels with {col}': f"{(0<df[f'm_{col}']).mean():.1%}",
    f'total m_{col}': f"{df[f'm_{col}'].sum()/1e6:,.1f} Mm",
    f'total ha_{col}_buf0m': f"{df[f'ha_{col}_buf0m'].sum()/1e6:,.1f} ha",
    f'total ha_{col}_buf4m': f"{df[f'ha_{col}_buf4m'].sum()/1e6:,.1f} ha",
    f'total ha_{col}_buf8m': f"{df[f'ha_{col}_buf8m'].sum()/1e6:,.1f} ha",
    f'total ha_{col}_buf12m': f"{df[f'ha_{col}_buf12m'].sum()/1e6:,.1f} ha",
}
