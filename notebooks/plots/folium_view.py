# Databricks notebook source
import os
from datetime import datetime

import folium
import numpy as np
import pandas as pd
from matplotlib import colormaps
from matplotlib.colors import to_hex

from elmo_geo.io import to_gdf
from elmo_geo.io.download import download_link


def get_n_colours(n: int, cmap="viridis") -> list[str]:
    cmap = colormaps[cmap].resampled(n)
    if hasattr(cmap, "colors"):
        colours = cmap.colors
    else:
        colours = np.array([colour for colours in cmap._segmentdata.values() for colour in colours])
    colours = np.apply_along_axis(to_hex, axis=1, arr=colours).tolist()
    while len(colours) < n:
        colours.extend(colours)
    return colours[:n]


def convert_datetime_to_string(x):
    if isinstance(x, (datetime, pd.Timestamp, np.datetime64)):
        return str(x)
    return x


# COMMAND ----------

tile = "NU13"
path = "/dbfs/mnt/lab-res-a1001004/restricted/elm_project/silver"

m, filepaths = folium.Map(), os.listdir(path)
for filepath, colour in zip(filepaths, get_n_colours(len(filepaths), "hsv")):
    f, name = f"{path}/{filepath}/sindex={tile}", filepath.replace(".parquet", "")
    try:
        df = pd.read_parquet(f).pipe(to_gdf).applymap(convert_datetime_to_string)
        df.explode(index_parts=True).explore(m=m, name=name, color=colour, show=False)
        print("+", name, sep="\t")
    except Exception as err:
        print("-", name, err, sep="\t")
folium.LayerControl().add_to(m)

f = f"/dbfs/FileStore/sindex={tile}.html"
m.save(f)
download_link(f, return_over_display=True)

m
