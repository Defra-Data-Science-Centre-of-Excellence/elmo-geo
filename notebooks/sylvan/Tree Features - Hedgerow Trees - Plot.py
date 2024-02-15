# Databricks notebook source
# MAGIC %pip install contextily rich

# COMMAND ----------

from typing import Optional, Tuple, List, Dict

import contextily as ctx
import geopandas as gpd
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.ticker import FuncFormatter, PercentFormatter
from pyspark.sql import functions as F
from shapely.geometry import Polygon
from elmo_geo.utils.types import SparkDataFrame, SparkSession
from tree_features import get_hedgerow_trees_features, make_parcel_geometries

#from elmo_geo import LOG, register
#from elmo_geo.io.io2 import load_geometry
#from elmo_geo.utils.dbr import spark

# COMMAND ----------

#register()
from sedona.spark import SedonaContext
SedonaContext.create(spark)

# COMMAND ----------

hedgerows_buffer_distance = 2
hedgerow_distance_threshold = 8

tree_detection_timestamp = "202311231323" # 20230602 timestamp was used for original mmsg figures 

elmo_geo_hedgerows_path = (
    "dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-hedge-2024_01_08.parquet"
)

adas_parcels_path = "dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet"

trees_output_template = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/"
    "tree_features/tree_detections/"
    "tree_detections_{timestamp}.parquet"
)
output_trees_path = trees_output_template.format(timestamp=tree_detection_timestamp)

features_output_template = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/tree_features_hr{threshold}_td{timestamp}.parquet"
)
parcel_trees_output = features_output_template.format(threshold = hedgerow_distance_threshold, timestamp=tree_detection_timestamp)

tile_to_visualise = "SP65nw"

save_data = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data

# COMMAND ----------

treesDF = (spark.read.parquet(output_trees_path)
           .repartition(200_000, "major_grid", "chm_path")
)
parcelsDF = (spark.read.format("geoparquet").load(adas_parcels_path)
             .repartition(1_250, "sindex")
)
hrDF = (spark.read.format("geoparquet").load(elmo_geo_hedgerows_path)
        .repartition(1_250, "sindex")
)
treeFeaturesDF = (spark.read.parquet(parcel_trees_output)
                  .select("id_parcel", "hrtrees_count2", "hedge_length")
                  .withColumn("hrtrees_per_m", F.col("hrtrees_count2") / F.col("hedge_length"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Filter to single tile for testing

# COMMAND ----------

# treesDF = treesDF.filter(f"chm_path like '%{tile_to_visualise}%'")
# hrtreesDF = hrtreesDF.filter(f"chm_path like '%{tile_to_visualise}%'")
# hrDFr = hrDFr.filter(f"REF_PARCEL_SHEET_ID like '{tile_to_visualise[:2]}%'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run hedgerow tree classification and get counts for plot

# COMMAND ----------

# Create geometry and parcel ID fields
pDF = (parcelsDF
            .withColumnRenamed("geometry", "geometry_parcel")
            .withColumnRenamed("id_parcel", "id_parcel_main")
            .select("id_parcel_main", "geometry_parcel")
)

hrDF = (hrDF
        .join(pDF, pDF.id_parcel_main == hrDF.id_parcel, "inner")
        .withColumn("geometry", F.expr(f"""
                                             ST_Intersection(
                                                 geometry,
                                                 ST_Buffer(geometry_parcel, {hedgerow_distance_threshold})
                                                 )"""))
        .withColumn("hedge_length", F.expr("ST_Length(geometry)"))
        .select("id_parcel", "geometry", "hedge_length", "sindex")
)

parcelsDF = make_parcel_geometries(parcelsDF)
treesDF = treesDF.withColumn("top_point", F.expr("ST_GeomFromWKT(top_point)"))
hrDF = (hrDF
        .withColumnRenamed("geometry", "geometry_hedge")
)

_hrtreesDF, _hrtreesPerParcelDF, counts = get_hedgerow_trees_features(
    spark,
    treesDF,
    hrDF,
    [hedgerows_buffer_distance],
    double_count = True,
    report_counts = True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Figures for all tiles
# MAGIC
# MAGIC ## Plot number of parcels by classification (contains hedgerows, contains hedgerow trees, etc)

# COMMAND ----------

n_unknown = 38592  # geometry difference method - comparing gaps in vom data to parcel geometries (I think)
# n_unknown = 7257 # vom tile comparison method

# COMMAND ----------

nParcels = parcelsDF.select("id_parcel").distinct().count()  # All parcels
nParcels

# COMMAND ----------

counts

# COMMAND ----------

nHRParcels = counts["hrtrees_count"][2]["nFeatureParcels"]
nHRTParcels = counts["hrtrees_count"][2]["nFeatureTreeParcels"]

parcel_count = pd.Series(
    index=["All Parcels", "HR Parcels", "HR Tree Parcels"],
    data=[nParcels, nHRParcels, nHRTParcels],
)
parcel_count

# COMMAND ----------

def stacked_bar_parcel_counts(
    data: pd.Series,
    title: str,
    n_unknown: Optional[int] = None,
) -> Tuple[plt.Figure, plt.Axes]:
    sns.set_palette("husl")
    sns.set_style("whitegrid")
    sns.set_context("talk")

    # Initialize the matplotlib figure
    f, ax = plt.subplots(figsize=(20, 6))

    # Plot the total crashes
    norm = mpl.colors.Normalize(-1, len(data) + 1)
    for i, (label, value) in enumerate(data.sort_values(ascending=False).items()):
        c_val = norm(i)
        p_val = value / data.sort_values(ascending=False).iloc[0]
        sns.barplot(x=[p_val], y=[""], label=label, color="#00A33B", alpha=c_val)

        y_pos = (((i % 2) * 2) - 1) * 0.05
        plt.text(p_val, y_pos, f"{value:,.0f}", fontdict={"fontsize": 20, "color": "#5b5b5b"})

    # Add unknown to the plot
    if n_unknown is not None:
        p_val = n_unknown / data.sort_values(ascending=False).iloc[0]
        sns.barplot(x=[p_val], y=[""], label="Unknown HR Tree Parcels", color="grey", alpha=1)

        plt.text(p_val, 0, f"{n_unknown:,.0f}", fontdict={"fontsize": 20, "color": "#5b5b5b"})

    # Add a legend and informative axis label
    ax.legend(ncol=2, loc="upper right", frameon=True)
    ax.set(ylabel="", xlabel="")
    sns.despine(left=True, bottom=True)
    # ax.grid(False)

    f.suptitle(
        title,
        x=0.09,
        y=1.03,
        ha="left",
        fontsize="large",
    )
    f.supxlabel(
        "Source: Environment Agency Vegitation Object Model"
        + r" $1m^2$"
        + " and Rural Payments Agency EFA Hedges",
        x=0.09,
        y=-0.07,
        ha="left",
        fontsize="small",
        wrap=True,
    )

    x_ticks = [0, 0.25, 0.5, 0.75, 1.0]
    ax.set_xticks(x_ticks)
    ax.xaxis.set_major_formatter(PercentFormatter(xmax=1))

    return f, ax


# COMMAND ----------

f, ax = stacked_bar_parcel_counts(
    parcel_count,
    title="Proportions of parcels with hedgerows and hedgerow trees",
    n_unknown=n_unknown,
)
f.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plot distribution of hedgerow trees per parcel

# COMMAND ----------

def plot_hrtree_dist(
    data: pd.Series,
    title: str,
    xlabel: str,
    max_quantile: float,
    dark: bool = False,
    dp: int = 0,
    bin_ratio: int = 1,
) -> Tuple[plt.Figure, plt.Axes]:
    """Plot the distribution of hedgerow trees
    Parameters:
        data: A pandas series of numbers of hedgerows trees on each parcel
        title: Title for the plot
        dark: Whether to style the plot with a dark background
    Returns: A tuple of the matplotlib figure and axes objects
    """
    # summarise the data
    count = data.shape[0]
    print(f"There are {count:,.0f} parcels in the dataset")
    mean = data.mean()
    print(f"Mean number of hedgerow trees {mean:.{dp}f}")
    median = data.median()
    print(f"Median number of hedgerow trees {median:.1f}")
    mq = data.quantile(max_quantile)
    print(f"{max_quantile} quantile number of hedgerow trees {mq:.3f}")

    n_above_max_quantile = data.loc[data > mq].shape[0]
    pcnt_above_max = 1 - max_quantile
    data_filtered = data.loc[data < mq]

    # define bins
    nvals = data_filtered.map(lambda x: int(np.round(x))).unique().shape[0]
    bins = np.linspace(0, nvals, (nvals * bin_ratio) + 1)

    sns.set_palette("husl")
    sns.set_style("whitegrid")
    sns.set_context("talk")

    if dark:
        sns.set_style("darkgrid")

    dark = False
    fig, ax = plt.subplots(figsize=(18, 6), constrained_layout=True)
    data_filtered.plot.hist(
        ax=ax, bins=bins, log=False, xlabel="f", alpha=0.7, linewidth=0, color="#00A33B"
    )
    # ax.xaxis.set_major_formatter(PercentFormatter(xmax=1))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:,.0f}"))
    c = "#999997" if dark else "#D9D9D6"
    ax.grid(True, which="minor", color=c, axis="y", linewidth=0.8)
    c = "#D9D9D6" if dark else "#999997"
    ax.grid(True, which="major", color=c, linewidth=1)
    # ax.minorticks_on()
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Parcel count")
    c = "white" if dark else "black"

    ax.axvline(x=median, ls="--", lw=2, color=c, alpha=0.5)
    ax.annotate(
        f"Median: {median:.{dp}f}\n(Mean: {mean:.{dp}f})",
        xy=(median, pd.cut(data_filtered, bins=bins).value_counts().iloc[0]),
        xycoords="data",
        # color="black",
        xytext=(4, 4),
        textcoords="offset pixels",
        ha="left",
    )
    ax.annotate(
        f"{n_above_max_quantile:,.0f} parcels > {mq:.1f}\n({pcnt_above_max:.1%} of parcels)",
        xy=(mq, pd.cut(data_filtered, bins=bins).value_counts().iloc[0]),
        xycoords="data",
        # color="black",
        xytext=(4, 4),
        textcoords="offset pixels",
        ha="right",
    )
    fig.suptitle(
        title,
        x=0.09,
        y=1.05,
        ha="left",
        fontsize="large",
    )
    fig.supxlabel(
        "Source: Environment Agency Vegitation Object Model"
        + r" $1m^2$"
        + " and Rural Payments Agency EFA Hedges",
        x=0.09,
        y=-0.09,
        ha="left",
        fontsize="small",
        wrap=True,
    )
    sns.despine(top=True, right=True, left=True, bottom=True)
    return fig, ax


# COMMAND ----------

data = treeFeaturesDF.toPandas()

# COMMAND ----------

for c in ["hedge_length", "hrtrees_count2"]:
    print(data[c].isnull().value_counts())

# COMMAND ----------

assert data.loc[ (data.hedge_length.isnull()) & (data.hrtrees_count2 > 0)].shape[0]==0
assert data.loc[data.hedge_length.notnull()].shape[0] > data.loc[data.hrtrees_count2 > 0].shape[0]

# COMMAND ----------

f, ax = plot_hrtree_dist(
    data["hrtrees_count2"],
    "Distribution of hedgerow trees - all parcels",
    "Number of hedgerow trees",
    0.995,
    dark=False,
    dp=1,
)
f.show()

# COMMAND ----------

f, ax = plot_hrtree_dist(
    data["hrtrees_per_m"].fillna(0) * 100,
    "Distribution of hedgerow tree density - all parcels",
    "Hedgerow trees per 100m of hedgerow",
    0.995,
    dark=False,
    dp=1,
    bin_ratio=3,
)
f.show()

# COMMAND ----------

f, ax = plot_hrtree_dist(
    data.dropna(subset="hedge_length")["hrtrees_per_m"] * 100,
    "Distribution of hedgerow tree density - all parcels with hedgerows",
    "Hedgerow trees per 100m of hedgerow",
    0.995,
    dark=False,
    dp=3,
    bin_ratio=3,
)
f.show()

# COMMAND ----------

f, ax = plot_hrtree_dist(
    data.loc[data['hrtrees_count2']>0, "hrtrees_per_m"] * 100,
    "Distribution of hedgerow tree density - all parcels with hedgerow trees",
    "Hedgerow trees per 100m of hedgerow",
    0.995,
    dark=False,
    dp=3,
    bin_ratio=3,
)
f.show()
