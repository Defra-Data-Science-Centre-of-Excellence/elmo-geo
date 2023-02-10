# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %pip install -qU sentinelsat beautifulsoup4

# COMMAND ----------

# reqs that are not needed...
# six cryptography pyopenssl sentinelsat beautifulsoup4 lxml

import datetime

from sentinelsat import SentinelAPI, LTATriggered

from elmo_geo.sentinel_api import sentinel_api_session
from elmo_geo.sentinel import is_downloaded, plot_products, extract_all_sentinel_data

tiles = [
    "30UUA",  # 2023 2022
    "30UUB",  # 2023 2022
    "30UVA",  # 2023 2022
    "30UVB",  # 2023 ...
    "30UVC",  # 2023 ...
    "30UVD",  # 2023
    "30UVE",  # 2023
    "30UVF",  # 2023
    "30UVG",  # 2023
    "30UWB",  # 2023
    "30UWC",  # 2023
    "30UWD",  # 2023
    "30UWE",  # 2023
    "30UWF",  # 2023
    "30UWG",  # 2023
    "30UXB",  # 2023
    "30UXC",  # 2023
    "30UXD",  # 2023
    "30UXE",  # 2023
    "30UXF",  # 2023
    "30UXG",  # 2023
    "30UYB",  # 2023
    "30UYC",  # 2023
    "30UYD",  # 2023
    "30UYE",  # 2023
    "31UCT",  # 2023
    "31UDT",  # 2023
    "31UDU",  # 2023
]
tile = "30UVC"
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
]

with sentinel_api_session() as api:
    products = api.query(date=(date_from, date_to), filename=f"*MSIL2A*T{tile}*")
    df = api.to_geodataframe(products)[keep_cols]

# COMMAND ----------

# add some columns and summarise
df["usefulpercentage"] = df.notvegetatedpercentage + df.vegetationpercentage
df["useful"] = df.usefulpercentage.rank(ascending=False) <= top_n
df["downloaded"] = df.filename.apply(
    is_downloaded
)  # this will only look for unzipped downloads - use extract_all_sentinel_data() to be sure they are all unzipped
print(df.groupby(["useful", "downloaded"])["filename"].count())

# COMMAND ----------

fig, _ = plot_products(
    df,
    title=f"Sentinel 2A products for tile {tile} between {date_from:%d/%m/%Y} and {date_to:%d/%m/%Y}",
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
        # max_attempts = 1,
        # lta_retry_delay=0,
    )
    # offline = 0
    # for product in to_download:
    #     try:
    #         api.download(product, directory_path='/dbfs/mnt/lab/unrestricted/sentinel/', checksum =False)
    #     except LTATriggered:
    #         api.trigger_offline_retrieval(product)
    #         offline += 1
    # if offline:
    #     print(f"LTA retrieval triggered for {offline} products")

# COMMAND ----------

extract_all_sentinel_data()

# COMMAND ----------
