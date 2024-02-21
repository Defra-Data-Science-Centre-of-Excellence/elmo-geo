# Databricks notebook source
# MAGIC %pip install -q osdatahub

# COMMAND ----------

import json
import os
import sys

from osdatahub import NGD

key = "WxgUdETn6cy58WZkfwZ7wdMVLlt5eDsX"
collection = "lnd-fts-land"
path = f"/dbfs/tmp/os/{collection}/"

# COMMAND ----------


def write(features, path, offset):
    with open(path + f"{offset}.geojson", "w") as f:
        f.write(json.dumps(features))


def get_offset():
    if os.path.exists(path + "offset"):
        with open(path + "offset") as f:
            offset = int(f.read())
    else:
        offset = 0
    return offset


def set_offset(offset):
    offset += 100
    with open(path + "offset", "w") as f:
        print(offset, file=f)
    return offset


def printnb(path, offset):
    sys.stdout.write(f"\rDownloading: {path} {offset:>8}.geojson")
    sys.stdout.flush()


def ngd_dl(key, collection, path):
    if not os.path.exists(path):
        os.makedirs(path)
    offset = get_offset()
    while True:
        printnb(path, offset)
        features = NGD(key, collection).query(offset=offset)
        n = len(features["features"])
        if n == 0:
            break
        write(features, path, offset)
        offset = set_offset(offset)


# COMMAND ----------

ngd_dl(key, collection, path)

# COMMAND ----------

from glob import glob

from geopandas import read_file
from pandas import DataFrame


def spark_read_geojsons(path, subset=[]):
    def read(filepath):
        df = read_file(filepath).to_wkb()
        if subset:
            df = df[subset]
        return df

    def read_udf(pdf):
        assert pdf.shape == (1, 1), f"ErrorInPDFShape: {pdf.shape}"
        return read(pdf.at[0, "filepath"])

    filepaths = glob(path + "*.geojson")
    schema = spark.createDataFrame(read(filepaths[0])).schema
    return DataFrame({"filepath": filepaths}).pipe(spark.createDataFrame).repartition("filepath").groupby("filepath").applyInPandas(read_udf, schema)


# COMMAND ----------

path = "/dbfs/tmp/os/lnd-fts-land/"
sdf = spark_read_geojsons(path, subset=[])

# COMMAND ----------

path = "/dbfs/tmp/os/lnd-fts-land/"
subset = ["osid", "theme", "description", "geometry"]
sdf = spark_read_geojsons(path, subset)
display(sdf)
sdf.count()

# COMMAND ----------

sdf.write.parquet("dbfs:/mnt/lab/unrestricted/elm_data/os/lnd-fts-land.parquet/", mode="overwrite")

# COMMAND ----------

from pyspark.sql import functions as F
from sedona.register import SedonaRegistrator

SedonaRegistrator.registerAll(spark)

# COMMAND ----------

sdf.withColumn("geometry", F.expr("ST_GeomFromWKB(geometry)")).display()
