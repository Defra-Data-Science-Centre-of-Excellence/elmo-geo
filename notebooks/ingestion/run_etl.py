# Databricks notebook source
"""Demo notebook for new ETL catalog."""
from elmo_geo import register
from elmo_geo.datasets import write_catalogue_json

register()

# TODO: Demo an sjoin M
# TODO: Add support for DAG plot Co
# TODO: Add support for non-geospatial Co
# TODO: Add support for changing the data type in the func and model validation Co

# COMMAND ----------

# write the catalog to json
write_catalogue_json()

# COMMAND ----------

# use with caution
#from elmo_geo.datasets import destroy_datasets # noqa
#destroy_datasets() # noqa
