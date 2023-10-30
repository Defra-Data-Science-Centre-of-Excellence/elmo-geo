# Databricks notebook source
# MAGIC %md
# MAGIC # Coastal
# MAGIC Farms with coastal habitat are coastal farms.
# MAGIC Future, parcels near the coast or tidal are coastal parcels.
# MAGIC
# MAGIC
# MAGIC ### Data
# MAGIC - NE: https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services
# MAGIC - ONS: https://services1.arcgis.com/ESMARspQHYMw9BZ9/ArcGIS/rest/services/
# MAGIC - Env: https://environment.data.gov.uk/apiportal
# MAGIC - BGC: https://geoportal.statistics.gov.uk/datasets/ons::countries-december-2021-boundaries-gb-bgc/explore?location=54.954659%2C-3.096273%2C12.83
# MAGIC - BFC: https://geoportal.statistics.gov.uk/datasets/ons::countries-december-2021-boundaries-gb-bfc/explore?location=54.954659%2C-3.096273%2C12.83
# MAGIC - BFE: https://geoportal.statistics.gov.uk/datasets/ons::countries-december-2021-boundaries-gb-bfe/explore?location=54.954659%2C-3.096273%2C12.83
# MAGIC - BUC: https://geoportal.statistics.gov.uk/datasets/ons::countries-december-2021-boundaries-gb-buc/explore?location=54.954659%2C-3.096273%2C12.83
# MAGIC
# MAGIC | Main_Habit | arable_grass | metric | total | mean |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | | | | | |

# COMMAND ----------

import numpy as np
import pandas as pd

# from pyspark.sql import functions as F

# COMMAND ----------

f_ph = "/dbfs/mnt/lab/unrestricted/elm/elmo/priority_habitats/output.feather"
f_wfm = "/dbfs/mnt/lab/unrestricted/elm/wfm/2023_06_09/wfm_parcels.feather"

df_ph = pd.read_feather(f_ph)
df_wfm = pd.read_feather(f_wfm)

# COMMAND ----------

df = pd.DataFrame.merge(
    df_ph,
    df_wfm[
        [
            "id_business",
            "id_parcel",
            "gm_parcel",
            "ha_parcel_geo",
            "ha_parcel_permanent_crops",
            "ha_parcel_permanent_pasture",
        ]
    ],
    on="id_parcel",
    how="outer",
)

threshold = 0.5 * df["ha_parcel_geo"]
df["arable_grass"] = np.where(
    threshold < df["ha_parcel_permanent_crops"],
    "arable",
    np.where(
        threshold < df["ha_parcel_permanent_pasture"],
        "grassland",
        "mixed",
    ),
)
df["gmpha"] = df["gm_parcel"] / df["ha_parcel_geo"]
df = df.drop(columns=["gm_parcel"])
df["ha_ph"] = df["ha_parcel_geo"] * df["proportion"]
df = df.drop(columns=["proportion"])


df

# COMMAND ----------

coastal = [
    "Coastal saltmarsh",
    "Coastal sand dunes",
    "Coastal vegetated shingle",
    "Mudflats",
    "Saline lagoons",
    "Maritime cliff and slope",
]
can_be_coastal = ["Coastal and floodplain grazing marsh"]

df_coastal = df[
    df["id_business"].isin(df[df["Main_Habit"].isin(coastal)]["id_business"])
]  # Businesses with Coastal Habitat
df_coastal = df_coastal[
    df_coastal["Main_Habit"].isin([*coastal, *can_be_coastal])
]  # Add Floodplain if a business has another coastal habitat
# df_coastal = df_coastal[df_coastal['id_business']!=67612]  # Remove National Trust
df_coastal = df_coastal.drop(columns=["id_business"])
df_grouped = df_coastal.groupby(["Main_Habit", "arable_grass"]).agg(["count", "sum", "mean"])

display(df_grouped.reset_index())

# COMMAND ----------

sf = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet"
sdf = spark.read.parquet(sf)

# sdf.filter(F.col('id_business').isin(out['id_business'].unique())).count()
