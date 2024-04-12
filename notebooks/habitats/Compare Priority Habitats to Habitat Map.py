# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Comparing Priority Habitats vs Living England Habitat Map
# MAGIC **Author**: Obi Thompson Sargoni
# MAGIC
# MAGIC Defra's Priority Habitas data and Living England's Habitat Map provide similar information.
# MAGIC
# MAGIC Both these datasets have been linked to WFM parcels (ADAS, November 2021). This notebook performs joins and mapping to compare theee sources of information on habitats.

# COMMAND ----------

import os
import pandas as pd
from datetime import datetime as dt
import glob

import elmo_geo
from elmo_geo.datasets.datasets import datasets
from elmo_geo.st import sjoin
from elmo_geo.st.geometry import load_geometry, load_missing

elmo_geo.register()
from pyspark.sql import functions as F

# COMMAND ----------

name = "priority_habitat_inventory"
version = "2021_03_26"
dataset = next(d for d in datasets if d.name == name)
[print(k, v, sep=":\t") for k, v in dataset.__dict__.items()]


# COMMAND ----------


