# Databricks notebook source
import requests
print('SSL: ', requests.get('https://sha256.badssl.com/').ok)

from sedona.spark import SedonaContext
print('Sedona: ', SedonaContext.create(spark))

import mosaic as mos
mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# MAGIC %sh
# MAGIC f=$(find /dbfs/cluster-logs/0410-141302-s79izw79/init_scripts/ -type f -name *.stderr.log | sort -n | tail -n1)
# MAGIC echo $f
# MAGIC cat $f

# COMMAND ----------

# MAGIC %sh
# MAGIC make fmt
# MAGIC make verify_dbr
