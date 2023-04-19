# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %pip install -Uq beautifulsoup4 rich lxml

# COMMAND ----------

import os

from elmo_geo.bare_soil import calc_bare_soil_percent
from elmo_geo.log import LOG
from elmo_geo.plot_bare_soil_dist import plot_bare_soil_dist
from elmo_geo.sentinel import sentinel_tiles, sentinel_years

# COMMAND ----------

versions = [v for v in os.listdir("/dbfs/mnt/lab/unrestricted/elm/sentinel/tiles")]
dbutils.widgets.dropdown("parcel version", versions[-1], versions)
dbutils.widgets.dropdown("tile", sentinel_tiles[0], sentinel_tiles)
dbutils.widgets.dropdown("year", sentinel_years[-1], sentinel_years)

version = dbutils.widgets.get("version")
tile = dbutils.widgets.get("tile")
year = int(dbutils.widgets.get("year"))


path_parcels = f"dbfs:/mnt/lab/unrestricted/elm/sentinel/tiles/{version}/parcels.parquet"
month_fm = f"{year-1}-11"
month_to = f"{year}-02"
path_ndvi = (
    "/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/ndvi/T{tile}-{month_fm}-{month_to}.tif".format(
        tile=tile, month_fm=month_fm, month_to=month_to
    )
)
ndvi_thresh = 0.25  # 0.15 0.2 0.25 0.3 0.35 0.4 0.6 0.7
# raster resolution - reproject to higher resolutions than 10m to speed up (but loose accuracy)
resolution = None
simplify = None  # geometry simplification tolerence - set this to speed up (but loose accuracy)
batch_size = 10000  # number of parcels to process in each batch
path_output = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/baresoil/"
    "bare_parcels/{tile}-{month_fm}-{month_to}.parquet".format(
        tile=tile, month_fm=month_fm, month_to=month_to
    )
)  # -{ndvi_thresh} , ndvi_thresh=ndvi_thresh
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(batch_size))

# COMMAND ----------

df = spark.read.parquet(path_parcels)
df = df.filter(df.tile == tile)
LOG.info(f"Tile {tile} contains {df.count():,.0f} parcels")
df

# COMMAND ----------

(
    df.withColumn(
        "bare_soil_percent",
        calc_bare_soil_percent(
            path_ndvi=path_ndvi,
            batch_size=batch_size,
            ndvi_thresh=ndvi_thresh,
        )(df.geometry),
    )
    .drop("proportion", "geometry")
    .write.format("parquet")
    .save(path_output, mode="overwrite")
)

# COMMAND ----------

result = spark.read.parquet(path_output).toPandas()
print(result.bare_soil_percent.describe())
fig, ax = plot_bare_soil_dist(
    data=result.bare_soil_percent,
    title=(
        f"Distribution of parcels in tile T{tile} by bare soil "
        f"cover November {year-1} - February {year}"
    ),
)
fig.show()

# COMMAND ----------

df

# COMMAND ----------
