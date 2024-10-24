# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Export datasets to S3
# MAGIC
# MAGIC Run the sync_datasets function to export all fresh, non-geographic DerivedDatasets to S3 for use by the elmo repository.

# COMMAND ----------

from elmo_geo.io.s3 import sync_datasets
from elmo_geo.datasets import catalogue

from elmo_geo import register
register()


# COMMAND ----------

sync_datasets(catalogue)
