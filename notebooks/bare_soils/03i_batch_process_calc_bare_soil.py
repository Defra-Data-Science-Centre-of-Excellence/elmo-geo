# Databricks notebook source
# MAGIC %md
# MAGIC # Run `03_calc_bare_soil_perc` using batch processing
# MAGIC
# MAGIC Running this notebook will end up running a notebook for multiple tiles and
# MAGIC years.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %pip install -U beautifulsoup4 lxml

# COMMAND ----------

import itertools
import os
from multiprocessing.pool import ThreadPool

from elmo_geo.batch_processing import run_with_retry
from elmo_geo.log import LOG
from elmo_geo.sentinel import sentinel_tiles, sentinel_years

dbutils.widgets.multiselect("years", sentinel_years[-1], sentinel_years)

# COMMAND ----------

years: list = [str(n) for n in dbutils.widgets.get("years").split(",")]
if len(years) < 1:
    raise ValueError("Please select at least one year to run")
#  only getting tiles that haven't been processed
done_tiles = [
    i.split("_")[2]  # i in form "hist_bare_{tile}_{year}"
    for i in os.listdir("/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/figures/")
    for j in years
    if i.startswith("hist_") and i.endswith(f"{j}.png")
]
tiles = [item for item in sentinel_tiles if item not in done_tiles][:6]
notebook = "03_calc_bare_soil_perc"

LOG.info(
    f"The tiles that are selected are: {tiles}\n"
    f"The years selected are: {years}\n"
    f"The notebook selected is: {notebook}"
)

# COMMAND ----------

items = ({"year": y, "tile": t} for y, t in itertools.product(years, tiles))

n_cpu = os.cpu_count()
LOG.info(f"Running on {n_cpu:,.0f} processes")
with ThreadPool(processes=n_cpu) as pool:
    pool.map_async(
        run_with_retry(
            notebook=notebook,
            timeout_seconds=21600,  # 6 hours before it times out
        ),
        # cross product to get all combinations of tile and year
        items,
    ).get()


# COMMAND ----------
