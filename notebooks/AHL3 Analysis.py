# Databricks notebook source
# MAGIC %md
# MAGIC # Add Hectarage to AHL3 Analysis
# MAGIC
# MAGIC ### Data
# MAGIC - [Emma Coffey - AHL3 Analysis](https://defra-my.sharepoint.com/:x:/g/personal/emma_coffey_defra_gov_uk/EaQd75mmJptMry5Rw6_47qUBo8HRKDkbHBwAnBLdptsKFw?wdOrigin=TEAMS-MAGLEV.p2p_ns.rwc&wdExp=TEAMS-TREATMENT&wdhostclicktime=1700135377391&web=1)
# MAGIC - rpa-parcels-adas
# MAGIC - wfm-field-2023_06_09
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC |      Metric      |  Count  | Difference |
# MAGIC | ---------------- | -------:| ----------:|
# MAGIC | AHL3 Rows        | 477,417 |            |
# MAGIC | AHL3 Parcels     | 451,773 |            |
# MAGIC | WFM matching     | 438,498 |     13,275 |
# MAGIC | RPA matching     | 448,065 |      3,708 |
# MAGIC | WFM/RPA matching | 448,083 |      3,690 |
# MAGIC
# MAGIC > Old
# MAGIC |      Metric      |  Count | Difference |
# MAGIC | ---------------- | ------:| ----------:|
# MAGIC | AHL3 Rows        | 87,729 |            |
# MAGIC | AHL3 Parcels     | 82,752 |            |
# MAGIC | WFM matching     | 80,091 |      2,661 |
# MAGIC | RPA matching     | 82,092 |        660 |
# MAGIC | WFM/RPA matching | 82,104 |        648 |
# MAGIC

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F

import elmo_geo
from elmo_geo.io import load_sdf

elmo_geo.register()

# COMMAND ----------

# df_ahl3 = pd.read_csv('/dbfs/FileStore/AHL3_analysis.csv')  # Old
df_ahl3 = pd.read_csv('/dbfs/FileStore/ecoffey_RITM0799970_2023_11_24.csv')
df_wfm = pd.read_feather('/dbfs/mnt/lab/restricted/ELM-Project/stg/wfm-field-2023_06_09.feather')
sdf_parcels = load_sdf('rpa-parcels-adas')

# COMMAND ----------

pdf_ahl3 = (df_ahl3
    [['APPLICATION_ID', 'PARCELREF']]
    .rename(columns={'APPLICATION_ID':'id_application', 'PARCELREF':'id_parcel'})
)
pdf_wfm = (df_wfm
    [['id_business', 'id_parcel', 'ha_parcel_geo', 'ha_parcel_uaa']]
    .rename(columns={'ha_parcel_geo':'ha_geo_wfm', 'ha_parcel_uaa':'ha_uaa_wfm'})
)
pdf_parcels = (sdf_parcels
    .select('id_parcel', F.expr('ST_Area(geometry)/10000 AS ha_geo_rpa'))
    .toPandas()
)


df_ha = pdf_wfm.merge(pdf_parcels, on='id_parcel', how='outer')
cols = [col for col in df_ha.columns if col.startswith('ha_')]
df_ha[[col+'_business' for col in cols]] = df_ha.groupby('id_business')[cols].transform(sum)


df = pdf_ahl3.merge(df_ha, on='id_parcel', how='left')


display(df)

# COMMAND ----------

{
    'rows': df.shape[0],
    'id_application': df['id_application'].notna().sum(),
    'id_parcel': df['id_parcel'].notna().sum(),
    'id_business': df['id_business'].notna().sum(),
    'wfm': df['ha_geo_wfm'].notna().sum(),
    'rpa': df['ha_geo_rpa'].notna().sum(),
    'wfm/rpa': df[['ha_geo_wfm', 'ha_geo_rpa']].notna().assign(x=lambda df: df['ha_geo_wfm'] | df['ha_geo_rpa'])['x'].sum(),
}
