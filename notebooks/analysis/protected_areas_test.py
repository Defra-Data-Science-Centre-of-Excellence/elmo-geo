# Databricks notebook source
from elmo_geo import register

register()

from elmo_geo.datasets import ne_sssi_units_raw

# COMMAND ----------

# check if data is fresh
ne_sssi_units_raw.is_fresh

# COMMAND ----------

# refresh the data if not
ne_sssi_units_raw.refresh()

# COMMAND ----------

from elmo_geo.datasets import ne_nnr_raw

# check if data is fresh
ne_nnr_raw.is_fresh

# COMMAND ----------

# refresh the data if not
ne_nnr_raw.refresh()
