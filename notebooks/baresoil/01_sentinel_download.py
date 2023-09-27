# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %pip install -qU sentinelsat beautifulsoup4

# COMMAND ----------

# reqs that are not needed...
# six cryptography pyopenssl sentinelsat beautifulsoup4 lxml

import datetime

from elmo_geo.r.sentinel import (
    extract_all_sentinel_data,
    is_downloaded,
    plot_products,
    sentinel_tiles,
)
from elmo_geo.io.sentinel_api import sentinel_api_session

tile = sentinel_tiles[0]
year = 2022
date_from = datetime.datetime(year=year - 1, month=11, day=1)
date_to = datetime.datetime(year=year, month=2, day=28)
top_n = 10  # number of products to keep
keep_cols = [
    "filename",
    "beginposition",
    "ondemand",
    "cloudcoverpercentage",
    "notvegetatedpercentage",
    "vegetationpercentage",
    "geometry",
    "size",
]

with sentinel_api_session() as api:
    products = api.query(date=(date_from, date_to), filename=f"*MSIL2A*T{tile}*")
    df = api.to_geodataframe(products)[keep_cols]

# COMMAND ----------

# add some columns and summarise
# df["usefulpercentage"] = df.notvegetatedpercentage + df.vegetationpercentage
df["size"] = [
    round(float(i.split(" ")[0])) / 1000
    if i.split(" ")[1] == "MB"
    else round(float(i.split(" ")[0]))
    for i in df["size"]
]
# mimicking updated usefulness (if size is small then not all image is there)
df["usefulpercentage"] = (df.notvegetatedpercentage + df.vegetationpercentage) * df["size"]
df["useful"] = df.usefulpercentage.rank(ascending=False) <= top_n
# this will only look for unzipped downloads
# use extract_all_sentinel_data() to be sure they are all unzipped
df["downloaded"] = df.filename.apply(is_downloaded)
print(df.groupby(["useful", "downloaded"])["filename"].count())

# COMMAND ----------

fig, _ = plot_products(
    df,
    title=(
        f"Sentinel 2A products for tile {tile} between {date_from:%d/%m/%Y}"
        f" and {date_to:%d/%m/%Y}"
    ),
    show_nmax=top_n,
)
fig.show()

# COMMAND ----------

to_download = df.loc[df.useful & ~df.downloaded, :].index.tolist()
print(f"Found {len(to_download):.0f} products to download... {to_download}")

# COMMAND ----------

with sentinel_api_session() as api:
    api.download_all(
        to_download,
        directory_path="/dbfs/mnt/lab/unrestricted/sentinel/",
    )

# COMMAND ----------

extract_all_sentinel_data()

# COMMAND ----------
