# Databricks notebook source
# MAGIC %md
# MAGIC # Combine years
# MAGIC
# MAGIC This notebook combines all the bare soil percent for all parcels for
# MAGIC all years.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %pip install -U beautifulsoup4

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.ticker import PercentFormatter

from elmo_geo import LOG
from elmo_geo.io import download_link
from elmo_geo.rs.sentinel import sentinel_years

# ensuring we can multiselect all years from the initial stage
all_string = "All"
years_to_choose = sentinel_years.copy()
years_to_choose.append(all_string)
dbutils.widgets.multiselect("years", all_string, years_to_choose)

# COMMAND ----------


def _read_file(year, path):
    """Reading parquet files for comare_years notebook"""
    return spark.read.parquet(path.format(year=year)).withColumnRenamed("bare_soil_percent", str(year))


years: list = [str(n) for n in sorted(dbutils.widgets.get("years").split(","))]
if all_string in years:
    years = sorted(sentinel_years)
if len(years) < 2:
    raise ValueError("Please select at least two years to run")
path = "/mnt/lab/unrestricted/elm/elmo/baresoil/output-{year}.parquet"
path_out = "/mnt/lab/unrestricted/elm/elmo/baresoil/output-all_years"

# COMMAND ----------

years_iter = iter(years)

df = _read_file(next(years_iter), path)
count = df.count()
for year in years_iter:
    df = df.join(
        _read_file(year, path).drop("tile"),
        on="id_parcel",
        how="outer",
    )
    if df.count() != count:
        LOG.warning(f"Dataset {year} had different parcel count.")
df.show()

# COMMAND ----------

df = df.toPandas()

# COMMAND ----------

# Count of parcels in each tile
df.groupby("tile", dropna=False).count().applymap(lambda x: f"{x:,.0f}")

# COMMAND ----------

# Check proportion of na values in each tile
df.groupby("tile", dropna=False)[[str(y) for y in years]].apply(lambda x: x.isna().sum() / x.count()).applymap(lambda x: f"{x:.2%}")

# COMMAND ----------

path_feather = f"/dbfs{path_out}.feather"
path_parquet = f"/dbfs{path_out}.parquet"
path_csv = f"/dbfs{path_out}.csv"

# convert types
df["tile"] = df["tile"].astype("category")

# output
df.to_feather(path_feather)
df.to_parquet(path_parquet)
df.to_csv(path_csv, index=False)
displayHTML(download_link(spark, path_feather))
displayHTML(download_link(spark, path_parquet))
displayHTML(download_link(spark, path_csv))

# COMMAND ----------

# plot the bare soil distributions over time


df = pd.read_parquet(f"/dbfs{path_out}.parquet").drop(columns=["id_parcel", "tile"])
sns.set_theme(context="talk", style="white")
fig, axs = plt.subplots(figsize=(22, 10), ncols=df.shape[1], constrained_layout=True, sharex=True, sharey=True)
colours = sns.color_palette("Dark2", n_colors=df.shape[1]).as_hex()
for ax, year, colour in zip(axs, sorted([x for x in df.columns]), colours, strict=True):
    ax.hist(df[year], bins=100, range=(0, 1), orientation="horizontal", log=True, lw=0, color=colour, alpha=0.8)
    ax.yaxis.set_major_formatter(PercentFormatter(1, 0))
    ax.set_ylim(0, 1)
    ax.set_title(f"{int(year)-1}-{year[2:]}", loc="left")
    ax.set_frame_on(False)
    ax.grid(True, which="major", color="#aaaaaa", axis="x", linewidth=0.8)
    ax.grid(True, which="minor", color="#dddddd", axis="x", linewidth=0.8)
    mean = df[year].mean()
    ax.axhline(mean, color="k", ls="--")
    ax.annotate(f"Mean: {mean:.1%}", (30, mean + 0.02))
fig.show()
