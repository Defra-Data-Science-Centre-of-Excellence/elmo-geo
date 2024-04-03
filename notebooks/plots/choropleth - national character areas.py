# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Choropleth Plot - National Charater Areas
# MAGIC
# MAGIC This notebook produces a choropleth plot

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %pip install -Uq seaborn mapclassify matplotlib

# COMMAND ----------

import geopandas as gpd
import mapclassify
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter, FuncFormatter
import numpy as np

import os
import geopandas as gpd
from functools import partial
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DoubleType, FloatType, IntegerType, LongType

from elmo_geo import LOG, register
from elmo_geo.datasets.datasets import datasets, parcels
from elmo_geo.st import sjoin 

register()

# COMMAND ----------

names = sorted([f"{d.source}/{d.name}/{v.name}" for d in datasets for v in d.versions])
dbutils.widgets.dropdown("dataset", names[-1], names)
_, name, version = dbutils.widgets.get("dataset").split("/")
dataset = next(d for d in datasets if d.name == name)
[print(k, v, sep=":\t") for k, v in dataset.__dict__.items()]

# present fields of the dataset to select which to plot
fields = spark.read.parquet(dataset.path_output.format(version=version)).schema.fields
numeric_variables = [field.name for field in fields if isinstance(field.dataType, (DecimalType, DoubleType, FloatType, IntegerType, LongType))]
dbutils.widgets.dropdown("plot variable", numeric_variables[0], numeric_variables)
value_column = dbutils.widgets.get("plot variable")
print(f"\nDataset variable to plot:\t{value_column}")

dbutils.widgets.text("variable name", "Variable Name")
dbutils.widgets.text("variable source", "Variable Source")

variable_name = dbutils.widgets.get("variable name")
variable_source = dbutils.widgets.get("variable source")
print(f"Variable name:\t{variable_name}")
print(f"Variable source:\t{variable_source}")

# COMMAND ----------

# Use national character areas as geometry to aggregate data to
path_nca = "dbfs:/mnt/lab/unrestricted/elm/defra/national_character_areas/2021_03_29/output.parquet"
path_nca_poly = "dbfs:/mnt/lab/unrestricted/elm/defra/national_character_areas/2021_03_29/polygons.parquet"

# COMMAND ----------

spark.read.parquet(dataset.path_output.format(version=version)).display()

# COMMAND ----------

df = (spark.read.parquet(dataset.path_output.format(version=version))
      .groupBy("id_parcel").agg(F.sum(F.col(value_column)).alias(value_column))
      ).toPandas()

mean = df[value_column].mean()
df.head()

# COMMAND ----------

df_nca = spark.read.parquet(path_nca).repartition(200).toPandas()
df_nca = df_nca.sort_values("proportion", ascending=False).drop_duplicates(subset=["id_parcel"]).drop(columns=[
#  "partition",
    "proportion"
])
df_nca

# COMMAND ----------

# check parcel counts in each NCA
df.set_index("id_parcel").join(df_nca.set_index("id_parcel"), how="inner").groupby("nca_name").count()[value_column].plot.hist(figsize=(20,6), bins=100)

# COMMAND ----------

# smallest NCAs by parcel count
df.set_index("id_parcel").join(df_nca.set_index("id_parcel"), how="inner").groupby("nca_name").count().sort_values(by=value_column, ascending=True).head(20)

# COMMAND ----------

# mean parcel bare soil % by NCA
df = df.set_index("id_parcel").join(df_nca.set_index("id_parcel"), how="inner").groupby("nca_name").mean()
df

# COMMAND ----------

polygons = (spark.read.parquet(path_nca_poly)
            .withColumn("geometry", F.expr("ST_GeomFromWKB(geometry)"))
            .groupby("nca_name")
            .agg(F.expr("ST_Union_Aggr(geometry) as geometry"))
            ).toPandas()
polygons = gpd.GeoDataFrame(polygons, crs = "epsg:27700").loc[:, ["nca_name", "geometry"]].set_index("nca_name")
polygons = polygons.join(df).reset_index().sort_values(by=value_column, ascending=False).dropna()
polygons

# COMMAND ----------

# join in the geometries
# polygons = gpd.read_file(path_nca_poly).loc[:, ["nca_name", "geometry"]].to_crs(27700).set_index("nca_name")
# polygons = polygons.join(df).reset_index().sort_values(by=value_column, ascending=False).dropna()
# polygons

# COMMAND ----------

# plot it
sns.set_style("white")
sns.set_context("paper")

fig, axes = plt.subplots(figsize=(12,8), nrows=2, ncols=2, constrained_layout=True, width_ratios=[2,1])
gs = axes[0, 0].get_gridspec()
for ax in axes[0:, 0]:
    ax.remove()
axbig = fig.add_subplot(gs[0:, 0])

polygons.plot(
    column=value_column,
    scheme="quantiles",
    cmap="Reds",
    legend=True,
    edgecolor="black",
    linewidth=0.5,
    legend_kwds={"fmt": "{:.1%}", "title":f"Mean {variable_name}"},  # Remove decimals in legend
    ax=axbig,
)
axbig.set_axis_off()
axbig.annotate(f"All of England mean: {mean:.2%}", xy=(0.01, 0.99), fontweight="bold", xycoords='axes fraction')

colors = [[plt.get_cmap("Reds", lut=5)(x) for x in np.linspace(0, 1, 5)][x] for x in mapclassify.Quantiles(polygons[value_column], k=5).yb]

n = 20
axes[0, 1].sharex(axes[1, 1])
head = polygons.head(n)
barcont = axes[0, 1].barh(y=head.nca_name, width=head[value_column], color=colors[:n], ec="black", lw=0.5)
axes[0, 1].set_title(f"Highest {n} areas by {variable_name}", loc="left")
axes[0, 1].invert_yaxis()
axes[0, 1].bar_label(
    barcont,
    head[value_column].map(lambda x: f"{x:.1%}"),
    padding=4,
)
axes[0, 1].get_xaxis().set_visible(False)

tail = polygons.tail(n)
barcont = axes[1, 1].barh(y=tail.nca_name, width=tail[value_column], color=colors[-n:], ec="black", lw=0.5)
axes[1, 1].set_title(f"Lowest {n} areas by {variable_name}", loc="left")
axes[1, 1].xaxis.set_major_formatter(PercentFormatter(xmax=1))
axes[1, 1].invert_yaxis()
axes[1, 1].bar_label(
    barcont,
    tail[value_column].map(lambda x: f"{x:.1%}"),
    padding=4,
)
axes[1, 1].get_xaxis().set_visible(False)
sns.despine(left=True, top=True, right=True, bottom=True)
fig.suptitle(
    f"{variable_name} for land parcels in England by National Character Area",
    x=0,
    ha="left",
    fontsize="x-large",
)
fig.supxlabel(
    f"Source: {variable_source}",
    x=0,
    ha="left",
    fontsize="small",
    wrap=True,
)
fig.show()
