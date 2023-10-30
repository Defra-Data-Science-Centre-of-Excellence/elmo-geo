# Databricks notebook source
# MAGIC %md
# MAGIC # Parcels, SSSI, CS challenge
# MAGIC
# MAGIC
# MAGIC ### Data Sources
# MAGIC I like to reduce datasets to be as minimal as possible.
# MAGIC I can recalculate area (in ha) and other metrics as the geometries are all using EPSG:27700.
# MAGIC - Reference Parcels - come from DASH, called rpa_geodata_mart-reference_parcels-2021_03_16
# MAGIC - SSSI Units - are from NE, [here](https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services/SSSI_Units_England/FeatureServer/0) (I like this website for the big long list, and it links the data.gov.uk page and esri's map viewer.)
# MAGIC - CS Agreements - todo
# MAGIC
# MAGIC ### DataFrames
# MAGIC parcels: id_parcel, geometry
# MAGIC sssi: id_sssi_unit, sssi_name, condition, geometry
# MAGIC cs: - todo
# MAGIC
# MAGIC ### Method
# MAGIC
# MAGIC W
# MAGIC
# MAGIC sjoin(

# COMMAND ----------

# MAGIC %sh
# MAGIC file=/dbfs/mnt/lab/restricted/ELM-Project/stg/ne-sssi_units-2023_02_10
# MAGIC url=https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services/SSSI_Units_England/FeatureServer/0
# MAGIC
# MAGIC # esri2geojson $url $file.geojson
# MAGIC # /databricks/miniconda/bin/ogr2ogr $file.parquet $file.geojson -t_srs EPSG:27700
# MAGIC

# COMMAND ----------

import pandas as pd

import elmo_geo

# from elmo_geo.st import sjoin
from elmo_geo.utils.misc import dbfs
from elmo_geo.utils.settings import FOLDER_ODS, FOLDER_STG

elmo_geo.register()

# COMMAND ----------

sf_parcel = dbfs(FOLDER_ODS, True) + "/rpa-parcels-2021_03_16.parquet"
f_farm = FOLDER_STG + "/wfm-farm-2023_06_09.feather"
f_field = FOLDER_STG + "/wfm-field-2023_06_09.feather"
f_sssi = FOLDER_STG + "/ne-sssi_units-2023_02_10.geojson"
sf_sssi = dbfs(FOLDER_STG, True) + "/ne-sssi_units-2023_02_10.parquet"


# COMMAND ----------

df_farm = pd.read_feather(f_farm)
df_field = pd.read_feather(f_field)


sdf_cs = spark.createDataFrame(
    pd.DataFrame.merge(
        df_field[["id_business", "id_parcel"]],
        df_farm[["id_business", "participation_schemes"]],
        on="id_business",
        how="outer",
    )
)


display(sdf_cs)

# COMMAND ----------

sdf_parcel = spark.read.format("geoparquet").load(sf_parcel)
sdf_sssi = spark.read.format("geoparquet").load(sf_sssi)


display(sdf_parcel)
display(sdf_sssi)
