# Databricks notebook source
# MAGIC %pip install -q git+https://github.com/aw-west-defra/cdap_geo.git

# COMMAND ----------

import geopandas as gpd
from fiona import listlayers

# COMMAND ----------

f_parcels_base = "/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_reference_parcels/format_GPKG_rpa_reference_parcels/SNAPSHOT_2023_02_07_rpa_reference_parcels/reference_parcels.zip/reference_parcels.gpkg"
sf_parcels_pq = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_02_07.parquet"
sf_parcels = "dbfs:/mnt/lab/unrestricted/elm/parcels.parquet"
sf_wfm = "dbfs:/mnt/lab/unrestricted/elm/wfm/v3.parquet"

# COMMAND ----------

layers = listlayers(f_parcels_base)
print(layers)
assert len(layers) == 1  # If not we need to repeat this for each layer

# COMMAND ----------

df_parcels_base = gpd.read_file(f_parcels_base)
df_parcels_base

# COMMAND ----------

df_parcels_base = read_gpkg(f_parcels_base)

# df_parcels_base.write.parquet(sf_parcels_pq, mode='overwrite')
display(df_parcels_base)

# COMMAND ----------

df_parcels = (
    spark.read.parquet(sf_wfm)
    .select(
        "id_business",
        "id_parcel",
    )
    .join(
        (
            spark.read.parquet(sf_parcels_pq).select(
                F.concat("SHEET_ID", "PARCEL_ID").alias("id_parcel"),
                "geometry",
            )
        ),
        how="left",
        on="id_parcel",
    )
)


# df_parcels.write.parquet(sf_parcels)
display(df_parcels)
