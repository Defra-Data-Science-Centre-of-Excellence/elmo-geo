# Databricks notebook source
from IPython.display import Markdown

display(Markdown(open("./readme.md").read()))

# COMMAND ----------

# MAGIC %run ./01_ingest
# MAGIC %run ./02_split
# MAGIC %run ./03_metrics

# COMMAND ----------

# MAGIC %sh du -sh /dbfs/mnt/lab/restricted/ELM-Project/*/*
