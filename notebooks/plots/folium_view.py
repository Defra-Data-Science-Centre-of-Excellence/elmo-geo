# Databricks notebook source
from decimal import Decimal
import os
import pandas as pd
import folium
import geopandas as gpd
from elmo_geo.io import to_gdf
from elmo_geo.io.download import download_link
from elmo_geo.utils.misc import dbfs
from pyspark.sql import functions as F, types as T
from elmo_geo.utils.types import SparkDataFrame


import numpy as np
from matplotlib import colormaps
from matplotlib.colors import to_hex

def get_n_colours(n: int, cmap="viridis") -> list[str]:
    cmap = colormaps[cmap].resampled(n)
    if hasattr(cmap, "colors"):
        colours = cmap.colors
    else:
        colours = np.array([colour
            for colours in cmap._segmentdata.values()
            for colour in colours
        ])
    colours = np.apply_along_axis(to_hex, axis=1, arr=colours).tolist()
    while len(colours) < n:
        colours.extend(colours)
    return colours[:n]


# COMMAND ----------

tile = "NU13"
path = "/dbfs/mnt/lab/restricted/ELM-Project/silver"

m, filepaths = folium.Map(), os.listdir(path)
for filepath, colour in zip(filepaths, get_n_colours(len(filepaths), "hsv")):
    f, name = f"{path}/{filepath}/sindex={tile}", filepath.replace(".parquet", "")
    try:
        df = pd.read_parquet(f).pipe(to_gdf)
        df.explode(index_parts=True).explore(m=m, name=name, color=colour, show=False)
        print("+", name, sep="\t")
    except Exception as err:
        print("-", name, err, sep="\t")
folium.LayerControl().add_to(m)

f = f"/dbfs/FileStore/sindex={tile}.html"
m.save(f)
download_link(f)

m
