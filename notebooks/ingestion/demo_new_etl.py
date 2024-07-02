# Databricks notebook source
# MAGIC %pip install -qU pandera[geopandas,pyspark] geopandas

# COMMAND ----------

"""Demo notebook for new ETL catalog."""

from elmo_geo import register
from elmo_geo.datasets import catalog, fc_sfi_agroforestry_raw, fc_sfi_agroforestry, write_catalog_json, destroy_datasets

register()

# TODO: Predicate pushdown M
# TODO: Demo an sjoin M

# TODO: Add support for DAG plot Co
# TODO: Add support for non-geospatial Co
# TODO: Add support for changing the data type in the func and model validation Co


# COMMAND ----------

fc_sfi_agroforestry.gdf

# COMMAND ----------

write_catalog_json()

# COMMAND ----------

# use with caution
# destroy_datasets()

# COMMAND ----------



# COMMAND ----------


