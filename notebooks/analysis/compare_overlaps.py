# Databricks notebook source
# MAGIC %md
# MAGIC # Compare Overlap Parcels methods
# MAGIC Compare the old method process_datasets with the task overlap from 04_prop.
# MAGIC
# MAGIC SSSI has a very small absolute difference which means they're very similar, and the relative difference is small and negative imply a slight increase in
# MAGIC capture with the new methodology.
# MAGIC Same with National Parks.  Most of the differences are very close to -1 or +1, which implies a difference in the parcels used.
# MAGIC
# MAGIC ### Conclusion: success

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df_sssi = (
    pd.read_feather("/dbfs/mnt/lab/unrestricted/elm/elmo/sssi/sssi.feather")  # old
    .merge(
        pd.read_parquet("/dbfs/mnt/lab/restricted/ELM-Project/silver/overlap-sssi_units-2024_03_07.parquet"),  # new
        on="id_parcel",
        how="outer",
    )
    .assign(
        proportion=lambda df: df["proportion"].fillna(0),
        proportion_0m=lambda df: df["proportion_0m"].fillna(0),
        diff=lambda df: df["proportion"] - df["proportion_0m"],
        abs_diff=lambda df: df["diff"].abs(),
    )
)

df_np = (
    pd.read_feather("/dbfs/mnt/lab/unrestricted/elm/elmo/national_park/national_park.feather")  # old
    .merge(
        pd.read_parquet("/dbfs/mnt/lab/restricted/ELM-Project/silver/overlap-national_park-2024_01_30.parquet"),  # new
        on="id_parcel",
        how="outer",
    )
    .assign(
        proportion=lambda df: df["proportion"].fillna(0),
        proportion_0m=lambda df: df["proportion_0m"].fillna(0),
        diff=lambda df: df["proportion"] - df["proportion_0m"],
        abs_diff=lambda df: df["diff"].abs(),
    )
)

# COMMAND ----------

df_sssi.hist("diff", bins=21)
df_sssi.describe()

# COMMAND ----------

df_np.hist("diff", bins=21)
df_np.describe()
