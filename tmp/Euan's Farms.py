# Databricks notebook source
# MAGIC %md
# MAGIC # Euan's Sheep
# MAGIC
# MAGIC ### Data
# MAGIC - euan-sheep-2023_11_09
# MAGIC - ons-ita3_cua-2023
# MAGIC ### Method
# MAGIC select, rename, and join
# MAGIC bin data
# MAGIC plot
# MAGIC ### Output
# MAGIC Several choropleth plots.

# COMMAND ----------

import contextily as ctx
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap

import elmo_geo

elmo_geo.register()

# COMMAND ----------

osm_provider = ctx.providers.OrdnanceSurvey.Light(key="WxgUdETn6cy58WZkfwZ7wdMVLlt5eDsX")

# COMMAND ----------

gdf = gpd.read_file("/dbfs/FileStore/ons_ita3_cua_2023.geojson").to_crs("epsg:3857").assign(geometry=lambda df: df.simplify(0.001))
gdf_countries = (
    gpd.read_file("/dbfs/FileStore/ons_countries_2022.gpkg").to_crs("epsg:3857").query('CTRY22NM == "England"').assign(geometry=lambda df: df.simplify(0.001))
)
pdf = pd.read_csv("/dbfs/FileStore/euan_jas_vulnerable_farm_share_2023_11_09.csv")

# COMMAND ----------

# Join
rename_gdf = {"ITL321NM": "County", "geometry": "geometry"}
rename_pdf = {"England 2021 County / Unitary Authority(2)": "County"}
cols = "Total farmed area", "Share under 50ha", "Share GL", "Share tenanted"

df = gpd.GeoDataFrame.merge(
    gdf.rename(columns=rename_gdf)[rename_gdf.values()],
    pdf.rename(columns=rename_pdf),
    on="County",
    how="outer",
).dropna(how="all")
for col in cols:
    df[col] = np.asfarray(df[col].replace("#DIV/0!", np.NaN).replace("#", np.NaN).astype(float))
    if col != "Total farmed area":
        df[col] = np.where(20_000 < df["Total farmed area"], df[col], np.NaN)


df

# COMMAND ----------

cols_bin = "Under 50ha Businesses", "Grazing Livestock Businesses", "Tenanted"

# Binnify
df["Under 50ha Businesses"] = pd.cut(
    df["Share under 50ha"],
    bins=[0, 0.05, 0.1, 0.15, 0.2, 0.25, np.inf],
    labels=["0%-5%", "5%-10%", "10%-15%", "15%-20%", "20%-25%", "25%+"],
)
df["Grazing Livestock Businesses"] = pd.cut(
    df["Share GL"],
    bins=[0, 0.2, 0.4, 0.6, 0.8, np.inf],
    labels=["0%-20%", "20%-40%", "40%-60%", "60%-80%", "80%+"],
)
df["Tenanted"] = pd.cut(
    df["Share tenanted"],
    bins=[0, 0.25, 0.3, 0.4, 0.5, np.inf],
    labels=["<25%", "25%-30%", "30%-35%", "35%-40%", "40%+"],
)


# Binnify (standard)
df["Under 50ha Businesses (std)"] = pd.cut(
    df["Share under 50ha"],
    bins=[0, 0.2, 0.4, 0.6, 0.8, np.inf],
    labels=["0%-20%", "20%-40%", "40%-60%", "60%-80%", "80%+"],
)
df["Grazing Livestock Businesses (std)"] = pd.cut(
    df["Share GL"],
    bins=[0, 0.2, 0.4, 0.6, 0.8, np.inf],
    labels=["0%-20%", "20%-40%", "40%-60%", "60%-80%", "80%+"],
)
df["Tenanted (std)"] = pd.cut(
    df["Share tenanted"],
    bins=[0, 0.2, 0.4, 0.6, 0.8, np.inf],
    labels=["0%-20%", "20%-40%", "40%-60%", "60%-80%", "80%+"],
)


df

# COMMAND ----------

cmap = LinearSegmentedColormap.from_list("redsish", plt.cm.get_cmap("YlOrRd")(np.linspace(0.2, 1, 256)))

# fig, axs = plt.subplots(1,3, figsize=(16,9))
axs = np.array([None] * 3)
for ax, col in zip(axs.flat, cols_bin):
    ax = df.plot(
        ax=ax,
        column=col,
        legend=True,
        cmap=cmap,
        edgecolor="k",
        linewidth=0.5,
        alpha=1,
        figsize=(9, 16),
    )
    ax = gdf_countries.plot(ax=ax, facecolor="none", edgecolor="k", linewidth=1)
    ax.set(title=col)
    ax.axis("off")
    ax.get_legend().set(title="Farm Area")
    # ctx.add_basemap(ax=ax, source=osm_provider)
None

# COMMAND ----------

cmap = LinearSegmentedColormap.from_list("redsish", plt.cm.get_cmap("YlOrRd")(np.linspace(0.2, 1, 256)))

# fig, axs = plt.subplots(1,3, figsize=(16,9))
axs = np.array([None] * 3)
for ax, col in zip(axs.flat, cols_bin):
    ax = df.plot(
        ax=ax,
        column=col + " (std)",
        legend=True,
        cmap=cmap,
        edgecolor="k",
        linewidth=0.5,
        alpha=1,
        figsize=(9, 16),
    )
    ax = gdf_countries.plot(ax=ax, facecolor="none", edgecolor="k", linewidth=1)
    ax.set(title=col)
    ax.axis("off")
    ax.get_legend().set(title="Farm Area")
    # ctx.add_basemap(ax=ax, source=osm_provider)
None

# COMMAND ----------

cmap = LinearSegmentedColormap.from_list("redsish", plt.cm.get_cmap("YlOrRd")(np.linspace(0.2, 1, 256)))

# fig, axs = plt.subplots(1,3, figsize=(16,9))
axs = np.array([None] * 3)
for ax, col in zip(axs.flat, cols_bin):
    ax = df.plot(
        ax=ax,
        column=col,
        legend=True,
        cmap=cmap,
        edgecolor="k",
        linewidth=0.5,
        alpha=0.7,
        figsize=(9, 16),
    )
    # ax = gdf_countries.plot(ax=ax, facecolor='none', edgecolor='k', linewidth=1)
    ax.set(title=col)
    ax.axis("off")
    ax.get_legend().set(title="Farm Area")
    ctx.add_basemap(ax=ax, source=osm_provider)
# fig.tight_layout()
None

# COMMAND ----------

plt.hist(df["Share under 50ha"])

# COMMAND ----------

plt.hist(df["Share tenanted"])

# COMMAND ----------

grazing_livestock_sorted = df.sort_values(by="Share GL", ascending=False)
grazing_livestock_sorted[["County", "Share GL"]]

# COMMAND ----------

farm_size_sorted = df.sort_values(by="Share under 50ha", ascending=False)
farm_size_sorted[["County", "Share under 50ha"]]

# COMMAND ----------

tenant_sorted = df.sort_values(by="Share tenanted", ascending=False)
tenant_sorted[["County", "Share tenanted"]]
