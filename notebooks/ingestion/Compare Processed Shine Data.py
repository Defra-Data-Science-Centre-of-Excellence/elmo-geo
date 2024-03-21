# Databricks notebook source
import numpy as np
import pandas as pd
import geopandas as gpd
from pyspark.sql import functions as F, types as T

import elmo_geo
from elmo_geo import LOG
from elmo_geo.io import to_gpq, download_link
from elmo_geo.io.file import st_simplify
from elmo_geo.st import sjoin
from elmo_geo.st.udf import st_union

elmo_geo.register()

# COMMAND ----------

shine_process_dataset = "/mnt/lab/unrestricted/elm/historic_england/shine/2022_12_30/output.parquet"
shine_process_dataset_12m = "/mnt/lab/unrestricted/elm/historic_england/shine/2022_12_30/output_elmo_geo_sjoin_12m.parquet"
shine_process_dataset_gs = "/mnt/lab/unrestricted/elm/historic_england/shine/2022_12_30/output_elmo_geo_sjoin.parquet"
elmo_geo_wall = "dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-wall-2024_01_26.parquet"

# COMMAND ----------

sdf_shine = spark.read.parquet(shine_process_dataset)
sdf_shine_12m = spark.read.parquet(shine_process_dataset_12m)
sdf_shine_gs = spark.read.parquet(shine_process_dataset_gs)
sdf_geo_wall = (spark.read.format("geoparquet").load(elmo_geo_wall)
                .filter(F.col("source") == "he-shine-2022_12_30")
)

# COMMAND ----------

sdf_shine.display()

# COMMAND ----------

sdf_geo_wall.display()

# COMMAND ----------

# Compare number of parcels with shine features
c_elmo = sdf_geo_wall.select(F.col("id_parcel")).dropDuplicates().count()
c_shine = sdf_shine.filter(F.col("proportion")>0).select(F.col("id_parcel")).dropDuplicates().count()
print(f"""
      Count of parcels with shine features.

      elmo-geo method: {c_elmo:,.0f}
      process dataset method: {c_shine:,.0f}
      """)

# COMMAND ----------

# How many of the parcels are shared
df_elmo_parcels = sdf_geo_wall.select(F.col("id_parcel")).dropDuplicates().toPandas()
df_shine_parcels = sdf_shine.filter(F.col("proportion")>0).select(F.col("id_parcel")).dropDuplicates().toPandas()

comp = pd.merge(df_elmo_parcels, df_shine_parcels, how = "outer", indicator=True)
print(comp._merge.value_counts())
