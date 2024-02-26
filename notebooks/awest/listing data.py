# Databricks notebook source
# MAGIC %sh  # DASH datasets
# MAGIC du -sh /dbfs/mnt/base/unrestricted/source_*/dataset_*

# COMMAND ----------

# MAGIC %sh  # ELM old
# MAGIC dirs=(
# MAGIC     /dbfs/mnt/lab/unrestricted/elm/*
# MAGIC     /dbfs/mnt/lab/unrestricted/elm_data/*
# MAGIC     /dbfs/mnt/lab/unrestricted/elm/buffer_strips/*
# MAGIC )
# MAGIC du -sh ${dirs[@]}

# COMMAND ----------

# MAGIC %sh  # ELM new
# MAGIC du -sh /dbfs/mnt/lab/restricted/ELM-Project/*/*

# COMMAND ----------

# MAGIC %sh  # DASH RPA parquet
# MAGIC dirs=(
# MAGIC     /dbfs/mnt/base/unrestricted/source_rpa
# MAGIC     /dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart
# MAGIC )
# MAGIC find ${dirs[@]} -type f -name *.parquet | parallel 'du -sh {}'
