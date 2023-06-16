# Databricks notebook source
# MAGIC %md
# MAGIC # Combine parcels and export
# MAGIC
# MAGIC This notebook combines all the bare soil percent for all parcels for
# MAGIC all tiles in a given year

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %pip install -U beautifulsoup4

# COMMAND ----------

import os
import re

import matplotlib.pyplot as plt
import pandas as pd  # QE F401
import seaborn as sns
from matplotlib.ticker import PercentFormatter

from elmo_geo.io import download_link
from elmo_geo.log import LOG
from elmo_geo.sentinel import sentinel_years

dbutils.widgets.dropdown("year", sentinel_years[-1], sentinel_years)

# COMMAND ----------

path = "/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/bare_parcels"
year = int(dbutils.widgets.get("year"))
month_fm = f"{year-1}-11"
month_to = f"{year}-02"
path_output = f"/mnt/lab/unrestricted/elm/elmo/baresoil/output-{year}.parquet"
pat = rf"^[0-9]{{{2}}}[A-Z]{{{3}}}-{month_fm}-{month_to}\.parquet$"
files = [f for f in os.listdir(path) if re.match(pat, f)]
LOG.info(f"Found {len(files)} matching files")
files

# COMMAND ----------

files_paths = [f"{path.replace('/dbfs','')}/{f}" for f in files]
files_iter = iter(files_paths)
df = spark.read.parquet(next(files_iter))
for f in files_iter:
    df = df.union(spark.read.parquet(f))
df.show()

# COMMAND ----------

LOG.info(f"There are {df.count():,.0f} processed parcels in the data")

# COMMAND ----------

dfp = df.toPandas()

# COMMAND ----------

dfp.groupby("tile").count()

# COMMAND ----------

# check nans
na_count = dfp.bare_soil_percent.isna().sum()
LOG.info(f"There are {na_count:,.0f} nan parcels in the data ({na_count/dfp.shape[0]:.2%})")

# COMMAND ----------

# remove duplicates (where a parcel was covered by more than one tile),
# keeping the one with the lowest bare soil
dfp = dfp.sort_values(by="bare_soil_percent", ascending=False).drop_duplicates(subset="id_parcel")
LOG.info(f"There are {dfp.shape[0]:,.0f} parcels in the data")

# COMMAND ----------

# check remaining nans
na_count = dfp.bare_soil_percent.isna().sum()
LOG.info(f"There are {na_count:,.0f} nan parcels in the data ({na_count/dfp.shape[0]:.2%})")

# COMMAND ----------

# summarise the data
count_no = dfp.loc[dfp.bare_soil_percent == 0, "id_parcel"].count()
LOG.info(f"There are {count_no:,.0f} parcels with no bare ground ({count_no/dfp.shape[0]:.2%})")
count_all = dfp.loc[dfp.bare_soil_percent == 1, "id_parcel"].count()
LOG.info(f"There are {count_all:,.0f} parcels with all bare ground ({count_all/dfp.shape[0]:.2%})")
LOG.info(f"The mean bare ground is {dfp.bare_soil_percent.mean():.2%}")

sns.set_palette("husl")
sns.set_style("whitegrid")
sns.set_context("talk")

fig, ax = plt.subplots(figsize=(12, 6), constrained_layout=True)
dfp.bare_soil_percent.plot.hist(ax=ax, bins=100, log=True, xlabel="f")
ax.xaxis.set_major_formatter(PercentFormatter(xmax=1))
ax.set_xlabel("PArcel count")
ax.set_xlabel("Bare soil percent")
fig.suptitle(
    f"Distribution of parcels by bare soil cover November {year -1} - February {year}",
    x=0,
    y=1.05,
    ha="left",
    fontsize="large",
)

sns.despine(top=True, right=True, left=True, bottom=True)
fig.show()

# COMMAND ----------

# save the data
(spark.createDataFrame(dfp).write.format("parquet").save(path_output, mode="overwrite"))

# COMMAND ----------

# show results
result = spark.read.parquet(path_output)
LOG.info(f"Rows: {result.count():,.0f}")
result.display()

# COMMAND ----------

# download
pandas_df = result.toPandas().drop(columns=["tile"])
path_feather = "/dbfs" + path_output.replace(".parquet", ".feather")
path_csv = "/dbfs" + path_output.replace(".parquet", ".csv")

# output
pandas_df.to_feather(path_feather)
pandas_df.to_csv(path_csv)
displayHTML(download_link(spark, path_feather))
displayHTML(download_link(spark, path_csv))

# COMMAND ----------
