# Databricks notebook source
# MAGIC %md
# MAGIC ### data
# MAGIC ##### raw
# MAGIC - sentinel 2
# MAGIC - parcel
# MAGIC ##### enriched
# MAGIC - f_ndvi/b_bare_soil raster
# MAGIC - bare soil = sjoin(parcel, ndvi)
# MAGIC   - id_parcel, bare soil
# MAGIC   - bare soil = 1-mean(ndvi)
# MAGIC
# MAGIC
# MAGIC null/empty parcel_geometry = 0
# MAGIC nan f_ndvi = 382
# MAGIC nan b_bare_soil
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install -q contextily

# COMMAND ----------

import elm_se

elm_se.register()
import contextily as ctx
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rioxarray as rxr
import seaborn as sns
from pyspark.sql import functions as F
from pyspark.sql import window as W

# COMMAND ----------

tile = "30UWE"
year = 2023
month_fm = f"{year-1}-11"
month_to = f"{year}-02"
version = "2023_02_07"

path_parcels = f"dbfs:/mnt/lab/unrestricted/elm/sentinel/tiles/{version}/parcels.parquet"
path_ndvi = f"/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/ndvi/T{tile}-{month_fm}-{month_to}.tif"

# COMMAND ----------

sf = "dbfs:/mnt/lab/unrestricted/elm/sentinel/tiles/2023_02_07/parcels.parquet/"
sf_out = "dbfs:/mnt/lab/unrestricted/elm/sentinel/tiles/2023_02_07/parcels-EPSG:32630.parquet"

batchsize = 500


sdf = (
    spark.read.parquet(sf)
    .withColumn(
        "geometry",
        F.expr('ST_AsBinary(ST_Transform(ST_GeomFromWKB(geometry), "EPSG:27700", "EPSG:32630"))'),
    )
    .withColumn(
        "key2",
        F.floor(F.row_number().over(W.Window.partitionBy("tile").orderBy("id_parcel")) / batchsize),
    )
)


display(sdf)
sdf.rdd.getNumPartitions()
sdf.write.parquet(sf_out, mode="overwrite", partitionBy=["tile", "key2"])

# COMMAND ----------

len(glob("/dbfs/mnt/lab/unrestricted/elm/sentinel/tiles/2023_02_07/parcels-EPSG:32630.parquet/*/*"))

# COMMAND ----------

da = rxr.open_rasterio(path_ndvi).squeeze()


sdf = spark.read.parquet(sf_out)
pdf = sdf.filter(f'tile=="{tile}"').toPandas()
gdf = gpd.GeoDataFrame(pdf, geometry=gpd.GeoSeries.from_wkb(pdf["geometry"], crs=27700), crs=27700)


# COMMAND ----------

fig, ax = plt.subplots(figsize=(6, 6), constrained_layout=True)
da.plot.imshow(ax=ax, vmin=-1, vmax=1, cmap=sns.color_palette("blend:blue,#FFFFFF,#00A33B", as_cmap=True))
ax.axis("off")
None

# COMMAND ----------

fig, ax = plt.subplots(figsize=(6, 6), constrained_layout=True)
gdf.plot(ax=ax, alpha=0.7)
ctx.add_basemap(ax=ax, crs=gdf.crs)
ax.axis("off")
None

# COMMAND ----------

threshold = 0.25


@F.pandas_udf("float")
def calc_bs(data):
    def fn(g):
        try:
            clipped = da.rio.clip_box(*g.bounds).rio.clip([g])
            return float((threshold < clipped).mean(skipna=True))
        except Exception:
            return -100

    return gpd.GeoSeries.from_wkb(data).apply(fn)


sdf = sdf.withColumn("bare_soil", calc_bs("geometry"))


display(sdf)

# COMMAND ----------

sdf.filter(F.isnan("bare_soil")).count()

# COMMAND ----------

from glob import glob

import geopandas as gpd
import numpy as np
import pandas as pd
import rioxarray as rxr


def fn(f, year, threshold):
    version = f.split("tiles/")[1].split("/")[0]
    tile = f.split("tile=")[1].split("/")[0]
    key2 = f.split("key2=")[1]
    f_ndvi = f"/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/ndvi/T{tile}-{year-1}-11-{year}-02.tif"
    f_out = f"/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/parcels.parquet/{version}-{year}-{tile}-{key2}.snappy.parquet"

    da = rxr.open_rasterio(f_ndvi).squeeze()

    pdf = pd.read_parquet(f)
    gdf = gpd.GeoDataFrame(pdf, geometry=gpd.GeoSeries.from_wkb(pdf["geometry"], crs=32630), crs=32630)

    def calc_bs(g):
        try:
            clipped = da.rio.clip_box(*g.bounds).rio.clip([g])
            return float((threshold < clipped).mean(skipna=True))
        except Exception:
            return np.nan

    gdf["bare_soil"] = gdf["geometry"].apply(calc_bs, da=da, threshold=0.25)
    gdf.to_parquet(f_out)
    return f"{f}, True\n"


version = "2023_02_07"

f = f"/dbfs/mnt/lab/unrestricted/elm/sentinel/tiles/{version}/parcels-EPSG:32630.parquet"
files = glob(f + "/*/*")
sc.parallelize(files).map(fn, year=2023, threshold=0.25).collect()
