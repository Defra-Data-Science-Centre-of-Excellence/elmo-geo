# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload
# MAGIC %pip install -U sentinelsat numpy==1.22.0 beautifulsoup4 lxml

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import rioxarray as rxr
import seaborn as sns

from elmo_geo.best_pixel import (
    finally_ndvi_cloud_prob,
    get_clean_image,
    process_ndvi_cloud_prob,
    replace_ndvi_cloud_prob,
)
from elmo_geo.raster import to_raster
from elmo_geo.sentinel import get_winter_datasets, sort_datasets_by_usefulness

# COMMAND ----------

list_of_tiles = ["30UWC", "30UYC", "30UYD", "30UVF", "30UWD", "30UXD", "30UWE", "30UXE", "30UXF"]
tile = f"T{list_of_tiles[0]}"  # "T30UUB"
year = 2023
datasets = get_winter_datasets(year, tile)
datasets = sort_datasets_by_usefulness(datasets)
datasets

# COMMAND ----------

ds = get_clean_image(
    datasets=datasets,
    process_func=process_ndvi_cloud_prob,
    replace_func=replace_ndvi_cloud_prob,
    finally_func=finally_ndvi_cloud_prob,
)


# COMMAND ----------

# save it
NODATA_VAL = np.nan
ndvi = ds["ndvi"].copy()
ndvi.name = "NDVI"
ndvi.rio.set_nodata(NODATA_VAL)
ndvi = ndvi.astype("f")  # smallest compatible float type
month_fm = f"{year-1}-11"
month_to = f"{year}-02"
path = f"/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/ndvi/{tile}-{month_fm}-{month_to}.tif"
to_raster(ndvi, path)

# COMMAND ----------

# plot it
ndvi = rxr.open_rasterio(path).squeeze()
sns.set_context("talk")
fig, ax = plt.subplots(figsize=(12, 12), constrained_layout=True)
ndvi.plot.imshow(
    ax=ax, vmin=-1, vmax=1, cmap=sns.color_palette("blend:blue,#FFFFFF,#00A33B", as_cmap=True)
)
ax.set_axis_off()
ax.set_title("")
# ax.set_facecolor("black")
# fig.set_facecolor("black")
footnote = f"Processed from Sentinel 2 imagery from November {year-1} to February {year}."
fig.suptitle(f"{tile}, winter {year}", x=0, y=1.01, ha="left", fontsize="large")
fig.supxlabel(footnote, x=0, y=-0.01, ha="left", fontsize="small")
fig.show()

# COMMAND ----------

# ds["tci"].plot.imshow(figsize=(30, 30))

# COMMAND ----------
