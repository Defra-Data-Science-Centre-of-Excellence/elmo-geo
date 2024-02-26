# Databricks notebook source
# MAGIC %md
# MAGIC # sc.parallelize demo
# MAGIC Distributed computing when you don't want a SparkDataFrames.
# MAGIC `sc.parallelize(list, partitions).map(fn).collect()`
# MAGIC
# MAGIC ### Benefits
# MAGIC - tasks can complete while others fail
# MAGIC - easy to code your usual python method
# MAGIC - no spark optimisations
# MAGIC
# MAGIC ### Problems
# MAGIC - no spark optimisations
# MAGIC - python is usually slower than sql
# MAGIC
# MAGIC
# MAGIC ### Alternatives
# MAGIC `[fn(f) for f in files]`  
# MAGIC `map(fn, files)`  
# MAGIC `multiprocessing.Pool().map(fn, files)`

# COMMAND ----------

from multiprocessing import Pool
from glob import glob
import geopandas as gpd


def fn(f):
    try:
        gdf = gpd.read_parquet(f)
        status = True
    except:
        status = False
    return status


# COMMAND ----------

files = glob('/dbfs/mnt/lab/restricted/ELM-Project/ods/rpa-parcels-adas.parquet/*')
len(files), files

# COMMAND ----------

files = files[:10]

# COMMAND ----------

[fn(f) for f in files]

# COMMAND ----------

list(map(fn, files))

# COMMAND ----------

Pool().map(fn, files)

# COMMAND ----------

rdd = sc.parallelize(files, len(files))
rdd.map(fn).collect()

# COMMAND ----------

assert all(_), [f for (f, r) in zip(files, _) if not r]
