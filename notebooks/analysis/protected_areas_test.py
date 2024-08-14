# Databricks notebook source
from elmo_geo import register

register()

# COMMAND ----------

from elmo_geo.datasets import ne_sssi_units_raw

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

# COMMAND ----------

from elmo_geo.datasets import ne_sac_raw

# check if data is fresh
ne_sac_raw.is_fresh

# COMMAND ----------

ne_sac_raw.refresh()

# COMMAND ----------

from elmo_geo.datasets import jncc_spa_raw

# check if data is fresh
jncc_spa_raw.is_fresh

# COMMAND ----------

jncc_spa_raw.refresh()

# COMMAND ----------

from elmo_geo.datasets import ne_ramsar_raw

# check if data is fresh
ne_ramsar_raw.is_fresh

# COMMAND ----------

# check if data is fresh
ne_ramsar_raw.refresh()

# COMMAND ----------

from elmo_geo.datasets import ne_marine_conservation_zones_raw

ne_marine_conservation_zones_raw.is_fresh


# COMMAND ----------

ne_marine_conservation_zones_raw.refresh()
