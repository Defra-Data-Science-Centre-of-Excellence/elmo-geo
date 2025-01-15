# Databricks notebook source
# MAGIC %pip install earthaccess calplot

# COMMAND ----------

import os

import calplot
import pandas as pd
import geopandas as gpd
from shapely import box
import earthaccess

from elmo_geo.utils.ssl import no_ssl_verification

USERNAME = "burrowsej"
PSWD = "7Fec@tcBs2r6"
tile = "003W55N"

auth = earthaccess.Auth()
auth.login() # set a strategy that works for you
if not auth.authenticated:
    raise AssertionError("Authentication failed")

def to_box(x: dict) -> box:
    polygon = x["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["GPolygons"][0]['Boundary']['Points']
    lats = [p["Latitude"] for p in polygon]
    longs = [p["Longitude"] for p in polygon]
    return box(min(longs), min(lats), max(longs), max(lats))

with no_ssl_verification():
    granules = earthaccess.granule_query().short_name("SPL2SMAP_S").readable_granule_name(f"*{tile}*").get()
if len(granules) == 0:
    raise AssertionError("No granules found")
df = (
    pd.DataFrame.from_dict(granules)
    .loc[lambda df: df.umm.map(lambda x: "RelatedUrls" in x)]
    .assign(
        url_download=lambda df: df.umm.map(lambda x: next(url for url in x["RelatedUrls"] if url["Type"] == "GET DATA")["URL"]),
        geometry=lambda df: gpd.GeoSeries(df.umm.map(to_box), crs=4326),
        granule=lambda df: df.umm.map(lambda x: x["DataGranule"]["Identifiers"][0]["Identifier"]),
        ease_grid_centre=lambda df: df.granule.str.split("_").map(lambda x: x[7]),
        smap_time=lambda df: df.granule.str.split("_").map(lambda x: pd.Timestamp(x[5])),
        s1_time=lambda df: df.granule.str.split("_").map(lambda x: pd.Timestamp(x[6])),
        version=lambda df: df.granule.str.split("_").map(lambda x: x[-2]),
        satmodepol=lambda df: df.granule.str.split("_").map(lambda x: x[4]),
        
        )
    .pipe(lambda df: gpd.GeoDataFrame(df, geometry="geometry"))
    .assign(geom_id = lambda df: df.geometry.map({v: k for k, v in dict(enumerate(df.geometry.unique(), start=1)).items()}))
    .drop(columns=["umm", "meta"])
)
#https://github.com/nasa-openscapes/earthdata-cloud-cookbook/blob/main/tutorials/earthaccess-demo.ipynb
df

# COMMAND ----------

df.drop_duplicates(subset="geometry").assign(geometry = lambda df: df.geometry.centroid).explore()

# COMMAND ----------

df.geom_id.unique()

# COMMAND ----------

df.set_index("s1_time").geom_id.resample("D").map(lambda x: x if len(x) == 1 else -1)

# COMMAND ----------

calplot.calplot(df.drop_duplicates(subset="granule").set_index("s1_time").geom_id, textformat='{:.0f}', edgecolor=None, cmap='Greens')
# calplot.calplot(df.drop_duplicates(subset="granule").set_index("s1_time").resample("D").granule.count(), textformat='{:.0f}', edgecolor=None, cmap='Greens')

# COMMAND ----------

# find relevant files
files = [f for f in os.listdir("/dbfs/mnt/lab/unrestricted/nasa/smap") if tile in f]
dates = pd.Series(1, index=pd.Index([pd.Timestamp(f.split("_")[5]) for f in files]))
events = dates.resample("D").count()

# COMMAND ----------

calplot.calplot(events, textformat='{:.0f}', edgecolor=None, cmap='Greens')
