# Databricks notebook source
import geopandas as gpd
import matplotlib.pyplot as plt

# COMMAND ----------

f_commons = "/dbfs/mnt/lab/restricted/ELM-Project/data/ne-commons-2020_12_21.geojson"
df_commons = gpd.read_file(f_commons).to_crs(27700)

df_commons

# COMMAND ----------

f_peat = "/dbfs/mnt/lab/restricted/ELM-Project/stg/defra-peaty_soils-2021_03_24.parquet"
df_peat = gpd.read_parquet(f_peat).to_crs(27700)

df_peat

# COMMAND ----------

df = df_peat.overlay(df_commons)

df

# COMMAND ----------

x = 10_000_000

(
    df_peat["shape_area"].sum() // x,
    df_commons["Shape__Area"].sum() // x,
    df.area.sum() // x,
    (df.area.sum() / df_commons["Shape__Area"].sum()).round(3),
)

# COMMAND ----------

fig, ax = plt.subplots(dpi=800)

df_peat.geometry.plot(ax=ax, alpha=0.5, color="tab:brown")
df_commons.geometry.plot(ax=ax, color="tab:blue")
df.geometry.plot(ax=ax, color="tab:purple")
ax.axis("off")

None
