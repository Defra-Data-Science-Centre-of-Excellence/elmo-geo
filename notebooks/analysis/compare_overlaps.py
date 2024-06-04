# Databricks notebook source
# MAGIC %md
# MAGIC # Compare Overlap Parcels methods
# MAGIC Compare the old method process_datasets with the task overlap from 04_prop.
# MAGIC
# MAGIC | dataset | t-test p value | accept |
# MAGIC |---|---|---|
# MAGIC | SSSI | 0.659 | null hypothesis, the datasets match |
# MAGIC | National Parks | 1.000 | null hypothesis, the datasets match |
# MAGIC
# MAGIC _Missing groupby name, this causes differences for SSSI Units because some parcels are in multiple._
# MAGIC
# MAGIC ### Conclusion: success

# COMMAND ----------

import pandas as pd
from scipy.stats import ttest_rel


def test_equal(df):
    """95% confidence ttest"""
    return ttest_rel(df['proportion'], df['proportion_0m'])[1]


# COMMAND ----------

f_sssi_old = "/dbfs/mnt/lab/unrestricted/elm/elmo/sssi/sssi.feather"
f_sssi_new = "/dbfs/mnt/lab/restricted/ELM-Project/silver/overlap-sssi_units-2024_03_07.parquet"
df_sssi = (
    pd.read_feather(f_sssi_old)
    .merge(pd.read_parquet(f_sssi_new), on="id_parcel")
    .assign(diff = lambda df: df["proportion"] - df["proportion_0m"])
)


f_np_old = "/dbfs/mnt/lab/unrestricted/elm/elmo/national_park/national_park.feather"
f_np_new = "/dbfs/mnt/lab/restricted/ELM-Project/silver/overlap-national_park-2024_01_30.parquet"
df_np = (
    pd.read_feather(f_np_old)
    .merge(pd.read_parquet(f_np_new), on="id_parcel")
    .assign(diff = lambda df: df["proportion"] - df["proportion_0m"])
)


# COMMAND ----------

print(f"t-test p_value: {test_equal(df_sssi)}")
df_sssi.hist("diff")
df_sssi.describe()

# COMMAND ----------

print(f"t-test p_value: {test_equal(df_np)}")
df_np.hist("diff")
df_np.describe()
