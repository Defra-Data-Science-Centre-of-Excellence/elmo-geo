# Databricks notebook source
# MAGIC %pip install contextily rich

# COMMAND ----------

from typing import Optional, Tuple

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
from tree_features import get_hedgerow_trees_features

#from elmo_geo import LOG, register
#from elmo_geo.io.io2 import load_geometry
from elmo_geo.utils.dbr import spark

# COMMAND ----------

#register()
from sedona.spark import SedonaContext
SedonaContext.create(spark)

# COMMAND ----------

hedgerows_buffer_distance = 2

timestamp = "202311231323" # 20230602 timestamp was used for original mmsg figures 

hedgerows_path = (
    "dbfs:/mnt/lab/unrestricted/elm_data/rural_payments_agency/efa_hedges/2022_06_24.parquet"
)

hedges_length_path = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/hedgerows_and_water/hedgerows_and_water.csv"
)

parcels_path = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_02_07.parquet"

path_output_template = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/"
    "hrtrees/tree_detections/"
    "{mtc}_tree_detections_{timestamp}.parquet"
)

hrtrees_output_template = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/" "hrtrees/hrtrees_{timestamp}_{buf}mbuffer.parquet"
)

parcel_hrtrees_template = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/"
    "hrtrees/hrtrees_per_parcel_{timestamp}_{buf}mbuffer.parquet"
)

parcel_hrtrees_csv_template = (
    "/dbfs/mnt/lab/unrestricted/elm/elmo/hrtrees/hrtrees_per_parcel_{timestamp}_{buf}mbuffer.csv"
)

trees_output_template = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/"
    "tree_features/tree_detections/"
    "tree_detections_{timestamp}.parquet"
)
output_trees_path = trees_output_template.format(timestamp=timestamp)

features_output_template = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/tree_features_{timestamp}.parquet"
)
parcel_trees_output = features_output_template.format(timestamp=timestamp)

tile_to_visualise = "SP65nw"

save_data = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data

# COMMAND ----------

treesDF = spark.read.parquet(output_trees_path)
treeFeaturesDF = (spark.read.parquet(parcel_trees_output)
                  .select("SHEET_PARCEL_ID", "hrtrees_count2")
)

hrDF = (spark.read.parquet(hedgerows_path)
        .withColumn("id_parcel", F.expr("Concat(REF_PARCEL_SHEET_ID, REF_PARCEL_PARCEL_ID)"))
        .withColumn("length", F.expr("ST_Length(ST_GeomfromWKB(geometry))"))
        .select("id_parcel", "length")
        .groupBy("id_parcel").agg(F.expr("SUM(length) as length"))
)
parcelsDF = spark.read.parquet(parcels_path)
hrLengthDF = spark.read.option("header", True).csv(hedges_length_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Select just one tile to test methods with

# COMMAND ----------

# treesDF = treesDF.filter(f"chm_path like '%{tile_to_visualise}%'")
# hrtreesDF = hrtreesDF.filter(f"chm_path like '%{tile_to_visualise}%'")
# hrDFr = hrDFr.filter(f"REF_PARCEL_SHEET_ID like '{tile_to_visualise[:2]}%'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run hedgerow tree classification and get counts for plot

# COMMAND ----------

# Create geometry and parcel ID fields
hrDF = hrDF.withColumn("wkb", load_geometry("geometry")).withColumn(
    "geometry", F.expr(f"ST_Buffer(geometry, {hedgerows_buffer_distance})")
)

treesDF = treesDF.withColumn("geometry", F.expr("ST_Point(top_x, top_y)"))

# Create unique parcel id
hrDF = hrDF.withColumn("SHEET_PARCEL_ID", F.concat("REF_PARCEL_SHEET_ID", "REF_PARCEL_PARCEL_ID"))
parcelsDF = parcelsDF.withColumn("SHEET_PARCEL_ID", F.concat("SHEET_ID", "PARCEL_ID"))

# COMMAND ----------

hrtreesDF, hrtreesPerParcelDF, counts = get_hedgerow_trees_features(
    spark,
    treesDF,
    hrDF,
    parcelsDF,
    hedgerows_buffer_distance,
)

# COMMAND ----------

hrLengthDF.show(5)

# COMMAND ----------

# Join with hedgerow length per parcel
hrtreesPerParcelDF = hrtreesPerParcelDF.join(
    hrLengthDF, hrtreesPerParcelDF.SHEET_PARCEL_ID == hrLengthDF.id_parcel, "left"
)
hrtreesPerParcelDF = hrtreesPerParcelDF.withColumn(
    "hrtrees_per_m", F.col("hrtrees_count") / F.col("hedge_length")
)

# COMMAND ----------

# Get total coverage of hr trees
area = hrtreesDF.select(
    F.expr(
        """
                                                        ST_Area(
                                                            ST_GeomFromWKT(crown_poly_raster)
                                                        )
                                                    """
    ).alias("crown_area")
)

area.select(F.sum(F.col("crown_area"))).show()

# COMMAND ----------

# save data
if save_data:
    hrtreesDF.write.mode("overwrite").parquet(hrtrees_path_output)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Figures
# MAGIC
# MAGIC ## Plot number of parcels by classification (contains hedgerows, contains hedgerow trees, etc)

# COMMAND ----------

n_unknown = 38592  # geometry difference method
# n_unknown = 7257 # vom tile comparison method

# COMMAND ----------

nParcels = parcelsDF.select("SHEET_PARCEL_ID").distinct().count()  # All parcels
counts["nParcels"] = nParcels

# COMMAND ----------

parcel_count = pd.Series(
    index=["All Parcels", "HR Parcels", "HR Tree Parcels"],
    data=[counts["nParcels"], counts["nHRParcels"], counts["nHRTParcels"]],
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

# Join hedgerow trees per parcel to hedgerow length per parcel
hrTreeDensity = (treeFeaturesDF
                 .join(
                     hrLengthDF, treeFeaturesDF.SHEET_PARCEL_ID == hrLengthDF.id_parcel, "left"
                 )
                 .withColumn("hrtrees_per_m", F.col("hrtrees_count2") / F.col("hedge_length"))
)

data = hrTreeDensity.toPandas()
data.shape

# COMMAND ----------

data.loc[ data['hrtrees_count2']==0, "hrtrees_count2"] = np.nan
data.dropna(subset="hrtrees_count2").hrtrees_per_m.describe()

# COMMAND ----------

# 99.99th percentile is 0.95. This is believable (1 tree per meter) but more than this likely an outlier
data.dropna(subset="hrtrees_count2").hrtrees_per_m.quantile(0.9999)

# COMMAND ----------

print(data.dropna(subset="hrtrees_count2").hrtrees_per_m.describe())

# COMMAND ----------

data.head()

# COMMAND ----------

# where do nulls come from?
# - come from parcels with hrtrees not looking up to enties in hedgerow length per parcel data.
print(data["SHEET_PARCEL_ID"].isnull().value_counts())
print(data["id_parcel"].isnull().value_counts())
print(data["hedge_length"].isnull().value_counts()) 
print(data["hrtrees_per_m"].isnull().value_counts())

# COMMAND ----------

if save_data:
    data.to_csv(parcel_hrtrees_csv_output)

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
    LOG.info(f"There are {count:,.0f} parcels in the dataset")
    mean = data.mean()
    LOG.info(f"Mean number of hedgerow trees {mean:.{dp}f}")
    median = data.median()
    LOG.info(f"Median number of hedgerow trees {median:.1f}")
    mq = data.quantile(max_quantile)
    LOG.info(f"{max_quantile} quantile number of hedgerow trees {mq:.3f}")

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
        f"{n_above_max_quantile} parcels > {mq:.1f}\n({pcnt_above_max:.1%} of parcels)",
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

f, ax = plot_hrtree_dist(
    data["hrtrees_count2"],
    "Distribution of hedgerow trees",
    "Number of hedgerow trees",
    0.995,
    dark=False,
    dp=1,
)
f.show()

# COMMAND ----------

f, ax = plot_hrtree_dist(
    data["hrtrees_per_m"] * 100,
    "Distribution of hedgerow tree density",
    "Hedgerow trees per 100m of hedgerow",
    0.995,
    dark=False,
    dp=1,
    bin_ratio=3,
)
f.show()

# COMMAND ----------

"""
parcelsDF = parcelsDF.withColumn("SPID", F.concat("SHEET_ID","PARCEL_ID"))
allDF = (parcelsDF
            .join(hrtreesPerParcelDF, parcelsDF.SPID==hrtreesPerParcelDF.SHEET_PARCEL_ID, 'left')
            .select(F.col("SPID").alias("SHEET_PARCEL_ID"), "hrtrees_count", "hrtrees_per_m")
            .fillna(0, subset=["hrtrees_count", "hrtrees_per_m"])
        )
allDF.columns
pdf_all = allDF.select("hrtrees_per_m").toPandas()

f, ax = plot_hrtree_dist(pdf_all["hrtrees_per_m"]*100, "Distribution of hedgerow tree density - all parcels", "Hedgerow trees per 100m of hedgerow", 0.995, dark = False, dp=3, bin_ratio=3)
f.show()
"""

# COMMAND ----------



# COMMAND ----------

hrParcelsDF = hrDF.withColumn(
    "SPID", F.concat("REF_PARCEL_SHEET_ID", "REF_PARCEL_PARCEL_ID")
).drop_duplicates()
allDF = (
    hrParcelsDF.join(
        hrtreesPerParcelDF, hrParcelsDF.SPID == hrtreesPerParcelDF.SHEET_PARCEL_ID, "left"
    )
    .select(F.col("SPID").alias("SHEET_PARCEL_ID"), "hrtrees_count2", "hrtrees_per_m")
    .fillna(0, subset=["hrtrees_count2", "hrtrees_per_m"])
    .drop_duplicates()
)

hrpdf = allDF.select("hrtrees_per_m").toPandas()

# COMMAND ----------

hrpdf2 = allDF.toPandas()

# COMMAND ----------

hrpdf.loc[hrpdf["hrtrees_count2"] == 0, "SHEET_PARCEL_ID"].drop_duplicates().shape

# COMMAND ----------

hrpdf_dd = hrpdf.drop_duplicates()
hrpdf_dd.shape

# COMMAND ----------

f, ax = plot_hrtree_dist(
    hrpdf_dd["hrtrees_per_m"] * 100,
    "Distribution of hedgerow tree density - all parcels with hedgerows",
    "Hedgerow trees per 100m of hedgerow",
    0.995,
    dark=False,
    dp=3,
    bin_ratio=3,
)
f.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Visualise spatial join

# COMMAND ----------

treesSubDF = treesDF.filter(f"chm_path like '%{tile_to_visualise}%'")
hrSubDF = hrDF.filter(f"REF_PARCEL_SHEET_ID like '{tile_to_visualise[:2]}%'")
hrtreesSubDF = hrtreesDF.filter(f"REF_PARCEL_SHEET_ID like '{tile_to_visualise[:2]}%'")

# COMMAND ----------

hrtreesSubDF.show(5)

# COMMAND ----------

# Define a bounding box to use for the map
xmin = 462000
xmax = 463000
ymin = 255000
ymax = 256000

bbox_coords = [(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax), (xmin, ymin)]

bbox = Polygon(bbox_coords)
gdfbb = gpd.GeoDataFrame({"geometry": [bbox]}, crs="EPSG:27700").to_crs("EPSG:3857")

# COMMAND ----------

# Convert to geopandas and plot

hrSubDF = hrSubDF.withColumn("wkb", F.col("geometry")).withColumn(
    "geometry", F.expr(f"ST_Buffer(ST_GeomFromWKB(wkb), {hedgerows_buffer_distance})")
)
treesSubDF = treesSubDF.withColumn("geometry", F.expr("ST_Point(top_x, top_y)"))

hrSubDF = hrSubDF.filter(
    F.expr(
        f"""
                                ST_Contains(
                                    ST_GeomFromWKT('{bbox.wkt}'),geometry
                                )
                                """
    )
)

treesSubDF = treesSubDF.filter(
    F.expr(
        f"""
                                ST_Contains(
                                    ST_GeomFromWKT('{bbox.wkt}'),geometry
                                )
                                """
    )
)

hrtreesSubDF = hrtreesSubDF.filter(
    F.expr(
        f"""
                                ST_Contains(
                                    ST_GeomFromWKT('{bbox.wkt}'),geom_right
                                )
                                """
    )
)

gdf_tile_trees = gpd.GeoDataFrame(treesSubDF.toPandas(), geometry="geometry")

ghrtreesDF = gpd.GeoDataFrame(hrtreesSubDF.toPandas(), geometry="geom_right")

ghrDFr = gpd.GeoDataFrame(hrSubDF.toPandas(), geometry="geometry")

# COMMAND ----------

ghrDFr_plot = ghrDFr.loc[ghrDFr["geometry"].map(lambda g: g.intersects(bbox))]

# COMMAND ----------

gdf_tile_trees.head()

# COMMAND ----------

f, ax = plt.subplots(figsize=(15, 15))

cmap = plt.get_cmap("gist_rainbow")
parcels = ghrDFr_plot["PARCEL_FK"].unique()
cmap_inputs = np.linspace(0, 1, len(parcels))
# parcel_colours = cmap(cmap_inputs)
dict_parcel_colours = dict(zip(parcels, cmap_inputs))
ghrDFr_plot["c"] = ghrDFr_plot["PARCEL_FK"].replace(dict_parcel_colours)

ghrDFr_plot.plot(ax=ax, facecolor="c", cmap=mpl.cm.gist_rainbow, alpha=0.5)


gdf_tile_trees.plot(ax=ax, markersize=45, alpha=1, marker="x", color="black")

ghrtreesDF.plot(ax=ax, markersize=50, alpha=1, marker="o", color="forestgreen")

ax.set_xlim(xmin=462000, xmax=463000)
ax.set_ylim(ymin=255000, ymax=256000)
ax.set_axis_off()
ctx.add_basemap(ax=ax, source=ctx.providers.OpenStreetMap.Mapnik, crs="EPSG:27700")

# COMMAND ----------

f, ax = plt.subplots(figsize=(15, 15))
ghrDFr_plot.plot(ax=ax, facecolor="cornflowerblue", alpha=0.5)
gdf_tile_trees.plot(ax=ax, markersize=45, alpha=1, marker="x", color="black")
ghrtreesDF.plot(ax=ax, markersize=50, alpha=1, marker="o", color="forestgreen")

ax.set_xlim(xmin=462000, xmax=463000)
ax.set_ylim(ymin=255000, ymax=256000)
ax.set_axis_off()

# COMMAND ----------

# Investigating hedges - are there duplicated geometries?
ghrDFr.FEATURE_ID.duplicated().value_counts()

# COMMAND ----------

ghrDFr.columns

# COMMAND ----------

ghrDFr["SHEET_PARCEL_ID"] = ghrDFr.apply(
    lambda row: f"{row['REF_PARCEL_SHEET_ID']}{row['REF_PARCEL_PARCEL_ID']}"
)
ghrDFr.SHEET_PARCEL_ID.duplicated().value_counts()

# COMMAND ----------

ghrDFr.geometry.duplicated().value_counts()

# COMMAND ----------

ghrDFr.PARCEL_FK.duplicated().value_counts()

# COMMAND ----------

ghrDFr.columns

# COMMAND ----------

gdfbb.to_crs(epsg=4326).total_bounds

# COMMAND ----------

bbox
