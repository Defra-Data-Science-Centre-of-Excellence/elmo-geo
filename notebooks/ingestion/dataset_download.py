# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Dataset Download
# MAGIC
# MAGIC Select a dataset to download and run the notebook to produce a download link.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from elmo_geo.datasets import catalogue
from elmo_geo.io.download import download_link

# COMMAND ----------

names = sorted([f"{d.name}" for d in catalogue])
default_name = names[0]
dbutils.widgets.dropdown("dataset", default_name, names)
name = dbutils.widgets.get("dataset")
dataset = next(d for d in catalogue if d.name == name)

[print(k, v, sep=":\t") for k, v in dataset.__dict__.items()]

# COMMAND ----------

download_link(dataset.path)
