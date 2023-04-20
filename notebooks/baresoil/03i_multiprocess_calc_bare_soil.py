# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %pip install -U beautifulsoup4 lxml

# COMMAND ----------

# MAGIC %md
# MAGIC # Run `calc_bare_soil_perc` using multiprocessing
# MAGIC
# MAGIC Running this notebook will end up running a notebook for multiple tiles and
# MAGIC years (comment out tiles and years where appropriate).

# COMMAND ----------

import itertools
import os
from multiprocessing.pool import ThreadPool

from elmo_geo.log import LOG
from elmo_geo.sentinel import sentinel_tiles, sentinel_years


# timeout_seconds - 1 hour
def run_with_retry(notebook: str, timeout_seconds: int = 3600, max_retries: int = 1):
    def _run_with_retry(args):
        num_retries = 0
        LOG.info(f"Starting {args}")
        while True:
            try:
                return dbutils.notebook.run(
                    path=notebook, timeout_seconds=timeout_seconds, arguments=args
                )
            except Exception:
                if num_retries > max_retries:
                    LOG.warning(f"Ran out of retries for {args}")
                    return
                else:
                    LOG.warning(f"Retrying error for {args}")
                    num_retries += 1
        LOG.info(f"Finished {args}")

    return _run_with_retry


# must use slicing to keep lists as lists
tiles = sentinel_tiles[:2]  # len(sentinel_tiles) = 28
years = sentinel_years[4:]  # len(sentinel_years) = 5
notebook = "03_calc_bare_soil_perc"

print(
    f"The tiles that are selected are: {tiles}\n"
    f"The years selected are: {years}\n"
    f"The notebook selected is: {notebook}"
)

# COMMAND ----------

if len(years) == 1 and len(tiles) == 1:
    print(f"Use {notebook} notebook to run with {years} and {tiles}")
elif len(years) == 1:
    items = ({"year": years[0], "tile": t} for t in tiles)
elif len(tiles) == 1:
    items = ({"year": y, "tile": tiles[0]} for y in years)
else:
    items = ({"year": y, "tile": t} for y, t in itertools.product(years, tiles))

n_cpu = os.cpu_count()
print(f"Running on {n_cpu:,.0f} processes")
with ThreadPool(processes=n_cpu) as pool:
    pool.map_async(
        run_with_retry(
            notebook=notebook,
        ),
        # cross product to get all combinations of tile and year
        items,
    ).get()


# COMMAND ----------
