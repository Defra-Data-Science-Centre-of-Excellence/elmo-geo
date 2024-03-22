# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch from Data Sources

# COMMAND ----------

import json
import os
import osdatahub
import pandas as pd
from elmo_geo.io import ingest_dash, ingest_esri, ingest_os, ingest_osm, ingest_spol


os.environ['OS_KEY'] = 'WxgUdETn6cy58WZkfwZ7wdMVLlt5eDsX'
os.environ['SPOL_USER'] = None
os.environ['SPOL_PASS'] = None


method = {
    "dash": ingest_dash,
    "esri": ingest_esri,
    "os": ingest_os,
    "osm": ingest_osm,
    "sharepoint": ingest_spol,
}


def search_available(df, string):
    display(df[df['name'].str.contains(string)==True])


os.chdir(os.getcwd().replace('/notebooks/ingestion', ''))

# COMMAND ----------



# COMMAND ----------

# ESRI

# COMMAND ----------

# OS
os_key = os.environ['OS_KEY']

osdatahub.DataPackageDownload(os_key, 4143).versions[0]

#download(26940, "/tmp/", None)

# COMMAND ----------

# OSM


# COMMAND ----------

# Manual
'''
rpa-parcel-adas
'''
