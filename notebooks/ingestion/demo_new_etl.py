# Databricks notebook source
"""Demo notebook for new ETL catalog."""
from elmo_geo import register
from elmo_geo.datasets import (
    destroy_datasets,
    fc_sfi_agroforestry,
    write_catalog_json,
)

register()

# TODO: Demo an sjoin M
# TODO: Add support for DAG plot Co
# TODO: Add support for non-geospatial Co
# TODO: Add support for changing the data type in the func and model validation Co

# COMMAND ----------

# MAGIC %pip install -q esridump

# COMMAND ----------

# import esridump
# url = "https://services2.arcgis.com/mHXjwgl3OARRqqD4/ArcGIS/rest/services/SFI_Silvo_Arable/FeatureServer/0"
# dumper = esridump.EsriDumper(url)

dumper.can_handle_pagination
timestamp = dumper.get_metadata()["editingInfo"]["dataLastEditDate"]
for feature in dumper:
    outfile.write(json.dumps(feature))
    outfile.write('\n')

# COMMAND ----------

# example of reading the geodataframe using `.gdf()` with filtering of columns and rows at read.
# Use `.sdf()` for a spark dataframe and `.df()` for a pandas df.
fc_sfi_agroforestry.gdf(columns=["geometry", "sensitivity"], filters=[("fid", "in", [1, 2, 3])])

# COMMAND ----------

# write the catalog to json
write_catalog_json()

# COMMAND ----------

# use with caution
destroy_datasets()
