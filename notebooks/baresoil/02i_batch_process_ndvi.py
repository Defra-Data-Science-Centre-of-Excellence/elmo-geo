# Databricks notebook source
# MAGIC %md
# MAGIC # Run `02_process_ndvi` using batch processing
# MAGIC
# MAGIC Running this notebook will end up running a notebook for multiple tiles and years

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
notebook = "02_process_ndvi"

LOG.info(
    f"The tiles that are selected are: {sentinel_tiles}\n"
    f"The years selected are: {years}\n"
    f"The notebook selected is: {notebook}"
)

# COMMAND ----------

items = ({"year": y, "tile": t} for y, t in itertools.product(years, sentinel_tiles))

n_cpu = os.cpu_count()
LOG.info(f"Running on {n_cpu:,.0f} processes")
with ThreadPool(processes=n_cpu) as pool:
    pool.map_async(
        run_with_retry(notebook=notebook),
        # cross product to get all combinations of tile and year
        items,
    ).get()


# COMMAND ----------
