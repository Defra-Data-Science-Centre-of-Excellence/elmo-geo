# Databricks notebook source
# MAGIC %md
# MAGIC # Add Hectarage to AHL3 Analysis
# MAGIC
# MAGIC |        Metric        |  Count | Difference |
# MAGIC | -------------------- | ------ | ---------- |
# MAGIC | AHL3 Rows            | 87,729 |            |
# MAGIC | AHL3 Parcels         | 82,752 |            |
# MAGIC | WFM matching         | 80,091 |      2,661 |
# MAGIC | Parcels matching     | 82,092 |        660 |
# MAGIC | WFM/Parcels matching | 82,104 |        648 |
# MAGIC
# MAGIC ### Data
# MAGIC - [Emma Coffey - AHL3 Analysis](https://defra-my.sharepoint.com/:x:/g/personal/emma_coffey_defra_gov_uk/EaQd75mmJptMry5Rw6_47qUBo8HRKDkbHBwAnBLdptsKFw?wdOrigin=TEAMS-MAGLEV.p2p_ns.rwc&wdExp=TEAMS-TREATMENT&wdhostclicktime=1700135377391&web=1)
# MAGIC - rpa-parcels-adas
# MAGIC - wfm-field-2023_06_09
# MAGIC
# MAGIC

# COMMAND ----------

82752-80091,\
82752-82092,\
82752-82104

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F

import elmo_geo
from elmo_geo.io import load_sdf

elmo_geo.register()

# COMMAND ----------

df_ahl3 = pd.read_csv('/dbfs/FileStore/AHL3_analysis.csv')
df_wfm = pd.read_feather('/dbfs/mnt/lab/restricted/ELM-Project/stg/wfm-field-2023_06_09.feather')
sdf_parcels = load_sdf('rpa-parcels-adas')

# COMMAND ----------

pdf_ahl3 = df_ahl3[['PARCELREF']].rename(columns={'PARCELREF':'id_parcel'})
pdf_wfm = df_wfm[['id_parcel', 'ha_parcel_geo', 'ha_parcel_uaa']]
pdf_parcels = sdf_parcels.select('id_parcel', F.expr('ST_Area(geometry)/10000 AS ha_geo')).toPandas()  # optimisation: filter ids

df = (pdf_ahl3
    .merge(pdf_wfm, on='id_parcel', how='left')
    .merge(pdf_parcels, on='id_parcel', how='left')
)

display(df)

# COMMAND ----------



# COMMAND ----------

{
    'rows': df.shape[0],
    'id_parcel': df['id_parcel'].notna().sum(),
    'wfm': df['ha_parcel_geo'].notna().sum(),
    'parcel': df['ha_geo'].notna().sum(),
    'wfm/parcel': df[['ha_parcel_geo', 'ha_geo']].notna().assign(x=lambda df: df['ha_parcel_geo'] | df['ha_geo'])['x'].sum(),
}

# COMMAND ----------


