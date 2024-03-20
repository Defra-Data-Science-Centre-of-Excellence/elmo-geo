# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Link wall geometries to parcels
# MAGIC
# MAGIC - elmo_geo-merged_wall: id_parcel, source, class, geometry
# MAGIC   - classes: wall, ruin, other
# MAGIC   - methodology: os + osm-wall + he-shine
# MAGIC
# MAGIC
# MAGIC Project: Wall
# MAGIC
# MAGIC Data Sources
# MAGIC
# MAGIC * os-lnd *tag=wall
# MAGIC osm-wall
# MAGIC     rpa-agreements (wall)
# MAGIC
# MAGIC Methodology
# MAGIC     wrangle
# MAGIC     concat
# MAGIC     sjoin 12m buf
# MAGIC
# MAGIC Output
# MAGIC
# MAGIC

# COMMAND ----------

import numpy as np
import pandas as pd
import geopandas as gpd
from pyspark.sql import functions as F, types as T

import elmo_geo
from elmo_geo import LOG
from elmo_geo.io import to_gpq, download_link
from elmo_geo.io.file import st_simplify
from elmo_geo.st import sjoin
from elmo_geo.st.udf import st_union

elmo_geo.register()

def log_info(sdf):
    LOG.info(f'''
        Count: {sdf.count():,}
        Partitions: {sdf.rdd.getNumPartitions():,}
        Columns: {sdf.columns}
    ''')
    return sdf

def cache(sdf):
    sdf.write.format('noop').save()
    return log_info(sdf)

simplify_tolerence: float = 0.5  # metres
max_vertices: int = 256  # per polygon (row)

# COMMAND ----------

# Wall
sdf_rpa_parcel = spark.read.format("parquet").load("dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet")

sdf_os_ngd_wall = (spark.read.format('parquet')
    .schema('layer string, theme string, description string, width double, geometry binary')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/os-ngd-2022.parquet')
    .filter('description LIKE "%Wall%"')
    .selectExpr(
        '"os-ngd-2022" AS source',
        # 'layer', 'theme', 'description',
        '''CASE
            WHEN LOWER(description) LIKE "%wall%" THEN "wall-wall"
            ELSE "wall-other"
        END AS class''',
        'ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry',
    )
)

sdf_os_boundary_wall = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/os-field_boundary-sample_v1_2.parquet')
    .filter('description == "Wall"')
    .selectExpr(
        '"os-field_boundary-sample_v1_2" AS source',
        # '"field_boundary" AS layer', 'theme', 'description',
        '''CASE
            WHEN LOWER(description) LIKE "%wall%" THEN "wall-wall"
            ELSE "wall-other"
        END AS class''',
        'ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry',
        # 'averagewidth_m AS m_width', 'averageheight_m AS m_height',
    )
)

sdf_osm_wall = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/osm-britain_and_ireland-2023_10_12.parquet')
    .selectExpr(
        '"osm-britain_and_ireland-2023_12_13" AS source',
        'barrier', 'other_tags',
        'ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry',
    )
    .filter('barrier IS NOT null OR other_tags LIKE \'%barrier"=>%\' OR other_tags LIKE \'%wall"=>%\'')
    .selectExpr(
        'source',
        '''CASE
            WHEN LOWER(barrier) LIKE "%wall%" THEN "wall-wall"
            ELSE "wall-other"
        END AS class''',
        'geometry',
    )
)

sdf_he_shine = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/he-shine-2022_12_30.parquet')
    .selectExpr(
        '"he-shine-2022_12_30" AS source',
        '''CASE
            WHEN shine_form != " " THEN "wall-relict"
            ELSE "wall-other"
        END AS class''',
        'ST_SetSRID(ST_GeomFromWKB(geom), 27700) AS geometry',
    )
)

# COMMAND ----------

sdf_wall = (sdf_os_ngd_wall
    .union(sdf_os_boundary_wall)
    .union(sdf_osm_wall)
    .union(sdf_he_shine)
    .withColumn('geometry', st_simplify())
    .withColumn('geometry', F.expr('EXPLODE(ST_Dump(geometry))'))
    .transform(lambda sdf: sjoin(sdf_rpa_parcel, sdf, rsuffix='', distance=12))
    .transform(st_union, ['source', 'id_parcel', 'class'], 'geometry')
    .transform(to_gpq, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-wall-2024_01_26.parquet')
)


display(sdf_wall)
