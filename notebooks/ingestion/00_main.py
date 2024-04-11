# Databricks notebook source
from IPython.display import Markdown
display(Markdown(open('./readme.md').read()))

# COMMAND ----------

# MAGIC %run ./01_datalists
# MAGIC %run ./02_fetch
# MAGIC %run ./03_sjoin
# MAGIC %run ./04_proportion
