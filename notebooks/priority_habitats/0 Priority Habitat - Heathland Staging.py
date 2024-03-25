# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Priority Habitat - Heathland Staging
# MAGIC
# MAGIC This notebook unions multiple Priority Habitat datasets adn filters this to provide a single parquet file containing all priority habitat healthland data.

# COMMAND ----------

import os
import geopandas as gpd
from functools import partial
from pyspark.sql import functions as F

from elmo_geo import LOG, register

register()

# COMMAND ----------

priority_habitat_directory = "/dbfs/mnt/lab/unrestricted/elm_data/defra/priority_habitat_inventory/"
version = "2021_03_26"

staging_output = f"dbfs:/mnt/lab/restricted/ELM-Project/stg/defra-priority-habitat-inventory-heathland-{version}.parquet"

# COMMAND ----------

ph_paths = [os.path.join(priority_habitat_directory, f) for f in os.listdir(priority_habitat_directory)]

# COMMAND ----------

sdfPH = spark.read.parquet(ph_paths[0].replace("/dbfs", "dbfs:"))

for p in ph_paths[1:]:
    sdf = spark.read.parquet(p.replace("/dbfs", "dbfs:"))
    assert sdf.columns == sdfPH.columns
    sdfPH = sdfPH.union(sdf)

sdf = None
sdfPH.display()

# COMMAND ----------

staging_output

# COMMAND ----------

(sdfPH
 .filter(F.expr("Main_Habit like '%heath%'"))
 .select("Main_Habit", "geometry")
 .repartition(200)
 .write.mode("overwrite").parquet(staging_output)
)
