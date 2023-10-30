# Databricks notebook source
# MAGIC %md
# MAGIC # Extract OSM layers
# MAGIC
# MAGIC

# COMMAND ----------

import elmo_geo

elmo_geo.register()

# COMMAND ----------

sdf = elmo_geo.io.datasets.load_sdf("osm-britain_and_ireland")

sdf.filter('other_tags LIKE \'%"%"=>"ha%ha"%\'')
