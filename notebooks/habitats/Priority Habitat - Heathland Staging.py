# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Priority Habitat - Heathland Staging
# MAGIC
# MAGIC This notebook filters the Priority Habitat dataset to provide a single parquet file containing all priority habitat healthland data.

# COMMAND ----------

from pyspark.sql import functions as F

from elmo_geo.io import to_pq_partitioned
from elmo_geo.st.geometry import load_geometry
from elmo_geo import register

register()

# COMMAND ----------

sf_priority_habitat = f"dbfs:/mnt/lab/unrestricted/elm_data/defra/priority_habitat_inventory/unified_2021_03_26.parquet"
sf_priority_habitat_heathland = f"dbfs:/mnt/lab/restricted/ELM-Project/stg/defra-priority_habitat_inventory_heathland-2021_03_26.parquet"

# COMMAND ----------

sdfPH = spark.read.parquet(sf_priority_habitat)

# COMMAND ----------

(sdfPH
 .filter(F.expr("Main_Habit like '%heath%'"))
 .withColumn("geometry", load_geometry("geometry"))
 .select(
     F.col("Main_Habit").alias("heath_habitat"), 
     "geometry",
     )
 .transform(to_pq_partitioned, sf_priority_habitat_heathland, mode = "overwrite")
)
