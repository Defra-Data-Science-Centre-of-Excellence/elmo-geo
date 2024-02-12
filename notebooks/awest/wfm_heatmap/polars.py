# Databricks notebook source
# MAGIC %pip install --pre geopolars

# COMMAND ----------

import polars as pl
import geopolars as gpl

# COMMAND ----------

help(gpl.convert.from_arrow)

# COMMAND ----------

f = '/dbfs/mnt/lab/restricted/ELM-Project/stg/rpa-parcel-adas.parquet'
df = pl.scan_parquet(f).select(
    pl.concat_str('RLR_RW_REFERENCE_PARCELS_DEC_21_SHEET_ID', 'RLR_RW_REFERENCE_PARCELS_DEC_21_PARCEL_ID').alias('id_parcel'),
    pl.col('RLR_RW_REFERENCE_PARCELS_DEC_21_VALID_FROM').str.to_datetime('%Y%m%d%H%M%s').alias('valid_from'),
    pl.col('RLR_RW_REFERENCE_PARCELS_DEC_21_VALID_TO').str.to_datetime('%Y%m%d%H%M%s').alias('valid_to'),
    pl.col('Shape').alias('geometry'),
    # gpl.convert.from_arrow(pl.col('geometry')).alias('geometry'),
)

df.head().collect()

# COMMAND ----------

f = '/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_reference_parcels/format_GPKG_rpa_reference_parcels/LATEST_rpa_reference_parcels/reference_parcels.gpkg'
df = gpl.read_file(f, max_features=10)
df
