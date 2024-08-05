# Databricks notebook source
"""Demo notebook for new ETL catalog."""
from elmo_geo import register
from elmo_geo.datasets import destroy_datasets, write_catalogue_json, reference_parcels
import pyspark.sql.functions as F

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
#destroy_datasets()
