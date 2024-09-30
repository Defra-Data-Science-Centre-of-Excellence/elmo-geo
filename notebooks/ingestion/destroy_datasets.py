# Databricks notebook source
from elmo_geo import register
from elmo_geo.datasets import destroy_datasets

register()

# COMMAND ----------

# use with caution
destroy_datasets()
