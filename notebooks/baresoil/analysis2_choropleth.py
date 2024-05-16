# Databricks notebook source
# MAGIC %md
# MAGIC # Choropleth
# MAGIC
# MAGIC This notebook holds the way to visualise England's bare soil in a given year.
# MAGIC [explain here]

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %pip install -U beautifulsoup4 lxml
# MAGIC %pip install -Uq seaborn mapclassify matplotlib

# COMMAND ----------

import geopandas as gpd
import mapclassify
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib.ticker import FuncFormatter, PercentFormatter  # noqa F401

from elmo_geo.rs.sentinel import sentinel_years
from elmo_geo.utils.dbr import spark, dbutils

dbutils.widgets.dropdown("year", sentinel_years[-1], sentinel_years)

# COMMAND ----------

year = int(dbutils.widgets.get("year"))
path = f"/mnt/lab/unrestricted/elm/elmo/baresoil/output-{year}.parquet"
path_nca = "dbfs:/mnt/lab/unrestricted/elm/elmo/national_character_areas/output.parquet"
path_nca_poly = (
    "/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/"
    "dataset_national_character_areas/format_SHP_national_character_areas/"
    "LATEST_national_character_areas/National_Character_Areas___Natural_England.shp"
)


# COMMAND ----------

df = spark.read.parquet(path).toPandas()
mean = df.bare_soil_percent.mean()
df

# COMMAND ----------

df_nca = spark.read.parquet(path_nca).repartition(200).toPandas()
df_nca = (
    df_nca.sort_values("proportion", ascending=False)
    .drop_duplicates(subset=["id_parcel"])
    .drop(columns=["proportion"])
)
df_nca

# COMMAND ----------

# check parcel counts in each NCA
df.set_index("id_parcel").join(df_nca.set_index("id_parcel"), how="inner").drop(
    columns="tile"
).groupby("nca_name").count()["bare_soil_percent"].plot.hist(figsize=(20, 6), bins=100)

# COMMAND ----------

# smallest NCAs by parcel count
df.set_index("id_parcel").join(df_nca.set_index("id_parcel"), how="inner").drop(
    columns="tile"
).groupby("nca_name").count().sort_values(by="bare_soil_percent", ascending=True).head(20)

# COMMAND ----------

# mean parcel bare soil % by NCA
df = (
    df.set_index("id_parcel")
    .join(df_nca.set_index("id_parcel"), how="inner")
    .drop(columns="tile")
    .groupby("nca_name")
    .mean()
)
df

# COMMAND ----------

# join in the geometries
polygons = (
    gpd.read_file(path_nca_poly)
    .loc[:, ["NCA_Name", "geometry"]]
    .rename({"NCA_Name": "nca_name"}, axis=1)
    .to_crs(27700)
    .set_index("nca_name")
)
polygons = (
    polygons.join(df).reset_index().sort_values(by="bare_soil_percent", ascending=False).dropna()
)
polygons

# COMMAND ----------

# plot it
sns.set_style("white")
sns.set_context("paper")

fig, axes = plt.subplots(
    figsize=(12, 8), nrows=2, ncols=2, constrained_layout=True, width_ratios=[2, 1]
)
gs = axes[0, 0].get_gridspec()
for ax in axes[0:, 0]:
    ax.remove()
axbig = fig.add_subplot(gs[0:, 0])

polygons.plot(
    column="bare_soil_percent",
    scheme="quantiles",
    cmap="Reds",
    legend=True,
    edgecolor="black",
    linewidth=0.5,
    legend_kwds={"fmt": "{:.1%}", "title": "Mean parcel bare soil %"},  # Remove decimals in legend
    ax=axbig,
)
axbig.set_axis_off()
axbig.annotate(
    f"All of England mean: {mean:.2%}", xy=(0.01, 0.99), fontweight="bold", xycoords="axes fraction"
)

colors = [
    [plt.get_cmap("Reds", lut=5)(x) for x in np.linspace(0, 1, 5)][x]
    for x in mapclassify.Quantiles(polygons.bare_soil_percent, k=5).yb
]

n = 20
axes[0, 1].sharex(axes[1, 1])
head = polygons.head(n)
barcont = axes[0, 1].barh(
    y=head.nca_name, width=head.bare_soil_percent, color=colors[:n], ec="black", lw=0.5
)
axes[0, 1].set_title(f"Highest {n} areas by mean bare soil %", loc="left")
axes[0, 1].invert_yaxis()
axes[0, 1].bar_label(
    barcont,
    head.bare_soil_percent.map(lambda x: f"{x:.1%}"),
    padding=4,
)
axes[0, 1].get_xaxis().set_visible(False)

tail = polygons.tail(n)
barcont = axes[1, 1].barh(
    y=tail.nca_name, width=tail.bare_soil_percent, color=colors[-n:], ec="black", lw=0.5
)
axes[1, 1].set_title(f"Lowest {n} areas by mean bare soil %", loc="left")
axes[1, 1].xaxis.set_major_formatter(PercentFormatter(xmax=1))
axes[1, 1].invert_yaxis()
axes[1, 1].bar_label(
    barcont,
    tail.bare_soil_percent.map(lambda x: f"{x:.1%}"),
    padding=4,
)
axes[1, 1].get_xaxis().set_visible(False)
sns.despine(left=True, top=True, right=True, bottom=True)
fig.suptitle(
    (
        f"Mean bare soil % for land parcels in England by National "
        f"Character Area, November {year-1} - February {year}"
    ),
    x=0,
    ha="left",
    fontsize="x-large",
)
fig.supxlabel(
    "Source: Sentinel-2 L2A imagery, bare soil defined where NDVI<0.25",
    x=0,
    ha="left",
    fontsize="small",
    wrap=True,
)
fig.show()

# COMMAND ----------

colors = [
    [plt.get_cmap("Reds", lut=5)(x) for x in np.linspace(0, 1, 5)][x]
    for x in mapclassify.Quantiles(polygons.bare_soil_percent, k=5).yb
]
colors

# COMMAND ----------
