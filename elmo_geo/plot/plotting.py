from typing import List, Tuple

import geopandas as gpd
import mapclassify
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import xarray as xr
from matplotlib.colors import ListedColormap
from matplotlib.ticker import FuncFormatter, PercentFormatter

from elmo_geo import LOG

dark_style = {
    "axes.facecolor": "black",
    "axes.edgecolor": "white",
    "axes.grid": True,
    "axes.axisbelow": "line",
    "axes.labelcolor": "white",
    "figure.facecolor": "black",
    "grid.color": "#141414",
    "text.color": "white",
    "xtick.color": "white",
    "ytick.color": "white",
    "patch.edgecolor": "white",
}


def plot_bare_soil_dist(data: pd.Series, title: str, dark: bool = False) -> Tuple[plt.Figure, plt.Axes]:
    """Plot the distribution of bare soil

    Parameters:
        data: A pandas series of bare soil proportions between 0-1 or na for each parcel
        title: Title for the plot
        dark: Whether to style the plot with a dark background

    Returns: A tuple of the matplotlib figure and axes objects
    """
    # summarise the data
    count = data.shape[0]
    LOG.info(f"There are {count:,.0f} parcels in the dataset")
    count_no = (data == 0).sum()
    LOG.info(f"There are {count_no:,.0f} parcels with no bare soil ({count_no/count:.2%})")
    count_all = (data == 1).sum()
    LOG.info(f"There are {count_all:,.0f} parcels with all bare soil ({count_all/count:.2%})")
    na_count = data.isna().sum()
    LOG.info(f"There are {na_count:,.0f} nan parcels in the data ({na_count/count:.2%})")
    mean = data.mean()
    LOG.info(f"The mean bare soil is {mean:.2%}")
    median = data.median()
    LOG.info(f"The median bare soil is {median:.2%}")

    sns.set_palette("husl")
    sns.set_style("whitegrid")
    sns.set_context("talk")

    if dark:
        sns.set_style("darkgrid", rc=dark_style)

    fig, ax = plt.subplots(figsize=(18, 6), constrained_layout=True)
    data.plot.hist(ax=ax, bins=100, log=True, xlabel="f", alpha=0.7, linewidth=0, color="#00A33B")
    ax.xaxis.set_major_formatter(PercentFormatter(xmax=1))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:,.0f}"))
    c = "#999997" if dark else "#D9D9D6"
    ax.grid(True, which="minor", color=c, axis="y", linewidth=0.8)
    c = "#D9D9D6" if dark else "#999997"
    ax.grid(True, which="major", color=c, linewidth=1)
    ax.minorticks_on()
    ax.set_xlabel("Proportion of bare soil")
    ax.set_ylabel("Parcel count")
    ax.set_xlim([0, 1])
    c = "white" if dark else "black"
    ax.axvline(x=mean, ls="--", lw=2, color=c)
    ax.annotate(
        f"Mean: {mean:.2%}",
        xy=(mean, pd.cut(data, bins=100).value_counts()[0]),
        xycoords="data",
        xytext=(4, 4),
        textcoords="offset pixels",
        ha="left",
    )
    fig.suptitle(
        title,
        x=0.09,
        y=1.05,
        ha="left",
        fontsize="large",
    )
    fig.supxlabel(
        f"Source: Sentinel-2 L2A imagery. No data for {na_count:,.0f} of {count:,.0f} parcels ({na_count/count:.3%}) " "due to cloud cover",
        x=0.09,
        y=-0.04,
        ha="left",
        fontsize="small",
        wrap=True,
    )
    sns.despine(top=True, right=True, left=True, bottom=True)
    return fig, ax


def plot_parcel_bare_soil(parcel_id: str, geometry: gpd.GeoSeries, ds: xr.Dataset, dark: bool = False) -> Tuple[plt.Figure, List[plt.Axes]]:
    """Produce a figure of subplots for a parcel showing its calculated NVDI, bare soil
        classification, true colour image, and cloud probability.

    Parameters:
        parcel_id: The id of the parcel for the plot's title
        geometry: The geometry of the parcel
        ds: The dataset of arrays including `ndvi`, `tci` and `cloud_prob`

    Returns:
        A tuple of the matplotlib figure and axes objects
    """
    ds_parcel = xr.Dataset(
        data_vars={
            "ndvi_clipped": ds["ndvi"].rio.clip(geometry, all_touched=False),
            "ndvi_boxed": ds["ndvi"].rio.clip_box(*geometry.bounds.iloc[0].tolist()),
            "cloud_prob": ds["cloud_prob"].rio.clip_box(*geometry.bounds.iloc[0].tolist()),
            "tci": ds["tci"].rio.clip_box(*geometry.bounds.iloc[0].tolist()),
        },
    )
    ds_parcel["vegetated"] = xr.where(ds_parcel["ndvi_clipped"] > 0.25, 1, 0)
    ds_parcel["vegetated"] = xr.where(ds_parcel["ndvi_clipped"].isnull(), ds_parcel["ndvi_clipped"], ds_parcel["vegetated"])

    if dark:
        sns.set_style("darkgrid", rc=dark_style)
    else:
        sns.set_style("white")
    sns.set_context(context="talk")

    fig, axes = plt.subplots(ncols=3, figsize=(30, 12), constrained_layout=True)
    imgs = list(range(3))

    # true colour image
    imgs[0] = ds_parcel["tci"].plot.imshow(ax=axes[0])
    geometry.plot(ax=axes[0], facecolor=(1, 1, 1, 0.0), linewidth=2, edgecolor=(1, 1, 1, 1))
    axes[0].set(title="")
    axes[0].set_title("True colour image (TCI)", loc="left")
    axes[0].set_axis_off()

    # nvdi
    imgs[1] = ds_parcel["ndvi_boxed"].plot.imshow(ax=axes[1], vmin=0.2, vmax=0.8)
    geometry.plot(ax=axes[1], facecolor=(1, 1, 1, 0.0), linewidth=2, edgecolor=(0, 0, 0, 1))
    axes[1].set(title="")
    axes[1].set_title("Normalised difference vegetation index (NDVI)", loc="left")
    axes[1].set_axis_off()
    imgs[1].colorbar.set_label("")

    # bare soil
    cmap = ListedColormap(["#D9262E", "#77BC1F"])  # Defra colors
    imgs[2] = ds_parcel["vegetated"].plot.imshow(ax=axes[2], cmap=cmap)
    geometry.plot(ax=axes[2], facecolor=(1, 1, 1, 0.0), linewidth=2, edgecolor=(1, 1, 1, 0))
    axes[2].set(title="")
    countall, countveg = float(ds_parcel["vegetated"].count()), float(ds_parcel["vegetated"].sum())
    bare_soil = (countall - countveg) / countall
    axes[2].set_title(f"Classification, {bare_soil:.1%} bare, threshold: NDVI < 0.25", loc="left")
    axes[2].set_axis_off()
    imgs[2].colorbar.set_label("")
    imgs[2].colorbar.set_ticks([1.0, 0.0])
    imgs[2].colorbar.set_ticklabels(["Vegetated", "Bare"])
    fig.suptitle(
        f"Parcel ID: {parcel_id}, Area: {float(geometry.area)/10000:,.1f}ha, " "Dates: November-December 2021, Resolution: 10m²",
        x=0.005,
        y=1.05,
        ha="left",
        fontsize="x-large",
    )
    return fig, axes


def plot_choropleth_with_head_and_tail_bars(
    gdf: gpd.GeoDataFrame,
    variable_column: str,
    variable_name: str,
    variable_source: str,
    plot_title: str,
) -> plt.Figure:
    """Creates a plot with three components: a choropleth and two horizontal bar charts.

    The choropleth maps the geometries in the input GeoDataFrame, coloured by the variable given by the 'variable_column' parameter. The two horizontal bar
    charts show the top 20 and bottom 20 rows of the GeoDataFrame. The bars are labeled by the index of the GeoDataFrame.

    Parameters:
        gdf: GeoDataFrame containing geometries and vaues to be plotted.
        variable_column: The name of GeoDataFrame coloumn to plot the values of.
        variable_name: The name of the variable to use in plot titles and legends.
        variable_source: The data source of the variable being plotted.
        plot_title: The title plot.

    Returns:
        A tuple of the matplotlib figure and axes objects
    """

    sns.set_style("white")
    sns.set_context("paper")

    fig, axes = plt.subplots(figsize=(12, 8), nrows=2, ncols=2, constrained_layout=True, width_ratios=[2, 1])
    gs = axes[0, 0].get_gridspec()
    for ax in axes[0:, 0]:
        ax.remove()
    axbig = fig.add_subplot(gs[0:, 0])

    mean = gdf[variable_column].mean()

    gdf.plot(
        column=variable_column,
        scheme="quantiles",
        cmap="Reds",
        legend=True,
        edgecolor="black",
        linewidth=0.5,
        legend_kwds={"fmt": "{:.1%}", "title": f"Mean {variable_name}"},  # Remove decimals in legend
        ax=axbig,
    )
    axbig.set_axis_off()
    axbig.annotate(f"All of England mean: {mean:.2%}", xy=(0.01, 0.99), fontweight="bold", xycoords="axes fraction")

    colors = [[plt.get_cmap("Reds", lut=5)(x) for x in np.linspace(0, 1, 5)][x] for x in mapclassify.Quantiles(gdf[variable_column], k=5).yb]

    n = 20
    axes[0, 1].sharex(axes[1, 1])
    head = gdf.head(n)
    barcont = axes[0, 1].barh(y=head.nca_name, width=head[variable_column], color=colors[:n], ec="black", lw=0.5)
    axes[0, 1].set_title(f"Highest {n} areas by {variable_name}", loc="left")
    axes[0, 1].invert_yaxis()
    axes[0, 1].bar_label(
        barcont,
        head[variable_column].map(lambda x: f"{x:.1%}"),
        padding=4,
    )
    axes[0, 1].get_xaxis().set_visible(False)

    tail = gdf.tail(n)
    barcont = axes[1, 1].barh(y=tail.nca_name, width=tail[variable_column], color=colors[-n:], ec="black", lw=0.5)
    axes[1, 1].set_title(f"Lowest {n} areas by {variable_name}", loc="left")
    axes[1, 1].xaxis.set_major_formatter(PercentFormatter(xmax=1))
    axes[1, 1].invert_yaxis()
    axes[1, 1].bar_label(
        barcont,
        tail[variable_column].map(lambda x: f"{x:.1%}"),
        padding=4,
    )
    axes[1, 1].get_xaxis().set_visible(False)
    sns.despine(left=True, top=True, right=True, bottom=True)
    fig.suptitle(
        plot_title,
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

    return fig
