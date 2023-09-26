# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

sf_wfm = 'dbfs:/mnt/lab-unrestricted/elm/wfm/v3.parquet'
sf_hedge = 'dbfs:/mnt/lab-unrestricted/elm/buffer_strips/hedgerows.parquet'

sdf_wfm = spark.read.parquet(sf_wfm)
sdf_hedge = spark.read.parquet(sf_hedge)

# COMMAND ----------

arable = [
  'ha_winter_wheat','ha_spring_wheat','ha_winter_barley','ha_spring_barley','ha_winter_oats','ha_spring_oats','ha_rapeseed_oil','ha_peas','ha_field_beans','ha_grain_maize','ha_potatoes','ha_sugar_beet','ha_other_crop',
  'ha_fodder_maize','ha_temporary_pasture',
]

grassland = [
  'ha_improved_grades_1_2',
  'ha_improved_grades_3_4_5',
  'ha_unimproved',
  'ha_improved',
  'ha_improved_disadvantaged',
  'ha_unimproved_disadvantaged',
  'ha_disadvantaged',
  'ha_severely_disadvantaged',
  'ha_moorland',
  'ha_fenland',
]

# COMMAND ----------

sdf = sdf_wfm.fillna(0).select(
  'id_business',
  'id_parcel',
  F.expr('+'.join(f'`{col}`' for col in arable)).alias('arable'),
  F.expr('+'.join(f'`{col}`' for col in grassland)).alias('grassland'),
).join(
  sdf_hedge.select('id_parcel', 'm_efa', 'm_adj').fillna(0),
  on = 'id_parcel',
)

display(sdf)

# COMMAND ----------

display(sdf
  .withColumn('p', F.expr('arable/(arable+grassland)'))
  .select(
    'arable',
    'grassland',
    'm_efa',
    F.expr('m_efa * p AS m_hedge_efa_arable'),
    F.expr('m_efa * (1-p) AS m_hedge_efa_grassland'),
    'm_adj',  
    F.expr('m_adj * p AS m_hedge_adj_arable'),
    F.expr('m_adj * (1-p) AS m_hedge_adj_grassland'),
  )
  .groupby().sum()
)
