# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %pip install -U beautifulsoup4 lxml

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import rioxarray as rxr
import seaborn as sns

from elmo_geo.best_pixel import (
    get_clean_image,
    process_ndvi_and_ndsi,
    replace_ndvi_low_ndsi,
)
from elmo_geo.raster import to_raster
from elmo_geo.sentinel import (
    get_winter_datasets,
    sentinel_tiles,
    sentinel_years,
    sort_datasets_by_time,
)

# COMMAND ----------

dbutils.widgets.dropdown("tile", sentinel_tiles[0], sentinel_tiles)
dbutils.widgets.dropdown("year", sentinel_years[-1], sentinel_years)

tile = dbutils.widgets.get("tile")
year = int(dbutils.widgets.get("year"))

datasets = get_winter_datasets(year, tile)
images = [str(n) for n in range(len(datasets))]

dbutils.widgets.dropdown("number of images to use", images[-4], images)

images_to_use = int(dbutils.widgets.get("number of images to use"))
datasets = sort_datasets_by_time(datasets)[:images_to_use]
datasets

# COMMAND ----------

ds = get_clean_image(
    datasets=datasets,
    process_func=process_ndvi_and_ndsi,
    replace_func=replace_ndvi_low_ndsi,
    sorting_algorithm=sort_datasets_by_time,
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
path = f"/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/ndvi/T{tile}-{month_fm}-{month_to}.tif"
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
footnote = f"Processed from Sentinel 2 imagery from November {year-1} to February {year}."
fig.suptitle(f"{tile}, winter {year}", x=0, y=1.01, ha="left", fontsize="large")
fig.supxlabel(footnote, x=0, y=-0.01, ha="left", fontsize="small")
fig.show()

# COMMAND ----------

# ds["tci"].plot.imshow(figsize=(30, 30))

# COMMAND ----------
