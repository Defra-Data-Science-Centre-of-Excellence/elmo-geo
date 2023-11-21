# Databricks notebook source
# MAGIC %md
# MAGIC # EVAST geospatial parcel features
# MAGIC
# MAGIC
# MAGIC ### Object
# MAGIC provide evast with hedge+ features for parcels, not geospatial  
# MAGIC remove polygon from buffer  
# MAGIC
# MAGIC ### Output
# MAGIC - water:  id_parcel, ha, ha_buf12m, ha_buf8m, ha_buf4m
# MAGIC - ditch:  id_parcel, ha, ha_buf12m, ha_buf8m, ha_buf4m
# MAGIC - hedge:  id_parcel, m, ha, ha_buf12m, ha_buf8m, ha_buf4m
# MAGIC - wood:  
# MAGIC - relict:  
# MAGIC - wall:  
# MAGIC - fence:  id_parcel, m
# MAGIC - UNIFIED: id_parcel, fence_m, hedge_m, hedge_ha, hedge_ha_buf12, hedge_ha_buf8, hedge_ha_buf4, water_ha, water_ha_buf12, water_ha_buf8, water_ha_buf4, ditch_ha, ditch_ha_buf12, ditch_ha_buf8, ditch_ha_buf4
# MAGIC
# MAGIC
# MAGIC ### Notes
# MAGIC -[] why does elmo_geo.io.file.convert_file not work but elmo_geo/io/ogr2gpq.sh does - check again with new '

# COMMAND ----------

import os
from glob import glob
from pyspark.sql import functions as F

import elmo_geo
from elmo_geo.utils.misc import dbfs
from elmo_geo.utils.settings import FOLDER_STG
from elmo_geo.io import ingest_dash, to_gpq, load_sdf
from elmo_geo.st import sjoin

elmo_geo.register()

# COMMAND ----------

sdf_rpa_parcel = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-parcels-adas.parquet')
    .select(
        F.expr('CONCAT(RLR_RW_REFERENCE_PARCELS_DEC_21_SHEET_ID, RLR_RW_REFERENCE_PARCELS_DEC_21_PARCEL_ID) AS id_parcel'),
        F.expr('ST_SetSRID(ST_GeomFromWKB(Shape), 27700) AS geometry'),
    )
)
to_gpq(sdf_rpa_parcel, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcels-adas.parquet')


sdf_rpa_hedge = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-hedge-2023_02_07.parquet')
    .select(
        F.expr('CONCAT(REF_PARCEL_SHEET_ID, REF_PARCEL_PARCEL_ID) AS id_parcel'),
        F.expr('ADJACENT_PARCEL_PARCEL_ID IS NOT NULL AS adj'),
        F.expr('ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry'),
    )
)
to_gpq(sdf_rpa_hedge, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-2023_02_07.parquet')


sdf_os_water = (spark.read.format('parquet')
    .option("mergeSchema", True)
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/os-ngd-2022.parquet/wtr_*')
    .select(
        'theme', 'description', 'watermark',
        F.expr('ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry'),
    )
)
to_gpq(sdf_os_water, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/os-water-2022.parquet')


# COMMAND ----------

def sgjoin_all(sdf, **kwargs):
    '''Spatial Groupby Join for SDFs
    '''
    for name, sdf_other in kwargs.items():
        sdf = sdf.join((
            sdf_other
            # sjoin(sdf, sdf_other, rsuffix='', how='left', distance=12)
            .select(
                'id_parcel',
                F.expr(
                    f'ST_MakeValid(ST_SimplifyPreserveTopology(ST_MakeValid(ST_PrecisionReduce(ST_MakeValid(ST_Buffer(geometry_{name}, 0.001)), 3)), 0.001)) AS geometry'
                ),  # AS Polygon
            )
            .groupby('id_parcel').agg(F.expr(
                f'ST_MakeValid(ST_Union_Aggr(geometry)) AS geometry_{name}'),
            )
        ), on='id_parcel', how='outer')
    return sdf


drains = ['Drain', 'Named Drain']

sdf_parcels = load_sdf('rpa-parcels-adas')
# sdf_hedge = load_sdf('rpa-hedge-2023_02_07')
# sdf_water = load_sdf('os-water').filter(~F.col('description').isin(drains))
# sdf_ditch = load_sdf('os-water').filter(F.col('description').isin(drains))
sdf_hedge = spark.read.format('parquet').load('dbfs:/mnt/lab/restricted/ELM-Project/out/hedge.parquet')
sdf_water = spark.read.format('parquet').load('dbfs:/mnt/lab/restricted/ELM-Project/out/water.parquet')
sdf_ditch = spark.read.format('parquet').load('dbfs:/mnt/lab/restricted/ELM-Project/out/ditch.parquet')

sdf = sgjoin_all(sdf_parcels, hedge=sdf_hedge, water=sdf_water, ditch=sdf_ditch)


display(sdf)
# to_gpq(sdf, 'dbfs:/mnt/lab/restricted/ELM-Project/out/parcel_geometries.parquet')

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get install -y -qq p7zip-full
# MAGIC cd data_sync
# MAGIC
# MAGIC 7z x rpa-hedge-adas.7z.001
# MAGIC 7z x rpa-hedge-2023_11_12.7z.001
# MAGIC 7z x rpa-parcels-2023_11_12.7z.001

# COMMAND ----------

# MAGIC %sh
# MAGIC # cd /Workspace/Repos/andrew.west@defra.gov.uk/elmo_geo/data_sync
# MAGIC cp -r data_sync /dbfs/FileStore

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs/FileStore/data_sync
# MAGIC 7z x rpa-hedge-2023_11_13.7z.001
# MAGIC 7z x rpa-hedge-adas.7z.001
# MAGIC ls data

# COMMAND ----------


