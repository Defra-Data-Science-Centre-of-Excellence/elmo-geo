# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Export datasets to S3
# MAGIC
# MAGIC Run the sync_datasets function to export all fresh, non-geographic DerivedDatasets to S3 for use by the elmo repository.

# COMMAND ----------

from elmo_geo import register
from elmo_geo.datasets import catalogue
from elmo_geo.io.s3 import sync_datasets

register()


# COMMAND ----------

filtered_catalogue = [d for d in catalogue if ("boundary" not in d.name) and ("esc" not in d.name)]
filtered_catalogue

# COMMAND ----------

sync_datasets(filtered_catalogue)
