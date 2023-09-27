# Databricks notebook source
# MAGIC %md
# MAGIC # Tile geometry dataset
# MAGIC This notebook is used to create the table that holds the sentinel tile names and their
# MAGIC geometries.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %pip install  beautifulsoup4 lxml

# COMMAND ----------

import geopandas as gpd
from shapely.ops import transform

from elmo_geo.sentinel import sentinel_tiles

# sentinel 2 tile geometries
path_source = "/dbfs/mnt/lab/unrestricted/elm_data/sentinel/sentinel_2_index_shapefile.shp"
path_destination = "/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/tiles.parquet"


# COMMAND ----------

df = (
    gpd.read_file(path_source)
    # filter to get required tiles (over England)
    .loc[lambda df: df.Name.isin(sentinel_tiles)]
    # remove Z coords
    .assign(
        geometry=lambda df: df.geometry.map(lambda g: transform(lambda x, y, z=None: (x, y), g))
    )
    .to_crs(27700)
    .reset_index(drop=True)
)
df

# COMMAND ----------

df.to_parquet(path_destination)

# COMMAND ----------
