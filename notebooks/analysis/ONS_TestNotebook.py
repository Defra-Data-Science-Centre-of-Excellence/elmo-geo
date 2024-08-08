# Databricks notebook source
from elmo_geo import register

register()

# COMMAND ----------

from elmo_geo.datasets import itl2_boundaries_raw

# check if data is fresh
itl2_boundaries_raw.is_fresh


# COMMAND ----------

# refresh the data if not
itl2_boundaries_raw.refresh()

# COMMAND ----------

from elmo_geo.datasets import itl2_boundaries_parcels

# check if data is fresh
itl2_boundaries_parcels.is_fresh

# COMMAND ----------

# refresh the data if not
itl2_boundaries_parcels.refresh()

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC rm - r /dbfs/mnt/lab/unrestricted/ELM-Project/silver/ons/itl2_boundaries-2024_07_24-4bea4832.parquet
