# Databricks notebook source
# MAGIC %sh
# MAGIC du -sh /dbfs/mnt/lab/restricted/ELM-Project/*/*

# COMMAND ----------

# MAGIC %sh
# MAGIC du -sh /dbfs/mnt/lab/unrestricted/elm/*
# MAGIC du -sh /dbfs/mnt/lab/unrestricted/elm_data/*
# MAGIC du -sh /dbfs/mnt/lab/unrestricted/elm/buffer_strips/*

# COMMAND ----------

# MAGIC %sh
# MAGIC du -sh /dbfs/mnt/base/unrestricted/source_*/dataset_*

# COMMAND ----------

# MAGIC %sh
# MAGIC find /dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/ -type f | xargs du -sh

# COMMAND ----------

/dbfs/mnt/base/unrestricted/
    source_rpa_spatial_data_mart/
    dataset_efa_control_layer/
    format_GPKG_efa_control_layer/
    SNAPSHOT_2023_06_27_efa_control_layer/
    LF_CONTROL_MV.zip/LF_CONTROL_MV/LF_CONTROL_MV.gpkg

/dbfs/mnt/base/unrestricted/
    source_rpa_sdm
    dataset_efa_control_layer
    format_gpkg
    snapshot_2023_06_27
        rpa_sdm-efa_control_layer-2023_06_27.gpkg
        rpa_sdm-efa_control_layer-2023_06_27.csv


# COMMAND ----------

# MAGIC %sh
# MAGIC dirs=(
# MAGIC   '/dbfs/mnt/base/unrestricted/source_rpa'
# MAGIC   '/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart'
# MAGIC )
# MAGIC find ${dirs[@]} -type f

# COMMAND ----------

# MAGIC %ls /dbfs/mnt/lab/restricted/ELM-Project/ods/

# COMMAND ----------

# MAGIC %sh
# MAGIC du -sh /dbfs/mnt/lab/restricted/ELM-Project/ods/elmo*

# COMMAND ----------



# COMMAND ----------

import elmo_geo
elmo_geo.register()

# COMMAND ----------

sdf = spark.read.format('geoparquet').load('dbfs:/mnt/lab/restricted/ELM-Project/stg/os-ngd-2022.parquet/')
display(sdf)

# COMMAND ----------


