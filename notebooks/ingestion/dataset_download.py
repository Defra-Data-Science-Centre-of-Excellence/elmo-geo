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

# COMMAND ----------

medallions = sorted({d.medallion for d in catalogue})
default_medallion = medallions[0]
dbutils.widgets.dropdown("A - Medallion", default_medallion, medallions)

# COMMAND ----------

medallion = dbutils.widgets.get("A - Medallion")
sources = sorted({d.source for d in catalogue if d.medallion == medallion})
default_source = sources[0]
dbutils.widgets.dropdown("B - Source", default_source, sources)

# COMMAND ----------

source = dbutils.widgets.get("B - Source")
datasets = sorted({d.name for d in catalogue if (d.medallion == medallion) and (d.source == source)})
default_dataset = datasets[0]
dbutils.widgets.dropdown("C - Dataset", default_dataset, datasets)

# COMMAND ----------

dataset = dbutils.widgets.get("C - Dataset")
dataset = next(d for d in catalogue if d.name == dataset)
_ = [print(k, v, sep=":\t") for k, v in dataset.__dict__.items()]

# COMMAND ----------

displayHTML(dataset.export())
