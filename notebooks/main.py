# Databricks notebook source
from elmo_geo import register

register()

# COMMAND ----------

# MAGIC %sh rm -r /dbfs/mnt/lab/restricted/ELM-Project/silver/*

# COMMAND ----------

dbutils.notebook.run("./ingestion/02_convert", 0)
dbutils.notebook.run("./boundary/Water", 0)
dbutils.notebook.run("./ingestion/03_sjoin", 0)

# COMMAND ----------

# MAGIC %sh ls /dbfs/mnt/lab/restricted/ELM-Project/silver
