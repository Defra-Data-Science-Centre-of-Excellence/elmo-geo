from typing import Iterator, Optional

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import rioxarray as rxr
import seaborn as sns
import xarray as xr
from matplotlib.ticker import PercentFormatter
from pyspark.sql import SparkSession
from rioxarray.exceptions import NoDataInBounds, OneDimensionalRaster
from shapely.geometry import Polygon
from xarray.core.dataarray import DataArray

from elmo_geo.log import LOG

# Define SparkSession and SparkContext
spark = SparkSession.getActiveSession()


def summarise_bare_soil(df: pd.DataFrame):
    """Log some summary metrics about the bare soils percentage
    Parameters:
        df: A pandas dataframe of bare soil data with an `output`
            column holding the percentages
    """

    LOG.info(f"Mean: {df.output.mean():.3%}")
    LOG.info(f"Median: {df.output.median():.3%}")
    LOG.info(f"Min: {df.output.min():.3%}")
    LOG.info(f"Max: {df.output.max():.3%}")

    nulls = df.output.isnull().sum()
    zeros = (df.output == 0).sum()
    ones = (df.output == 1).sum()
    count = df.output.size
    other = count - zeros - ones - nulls

    LOG.info(f"Total count: {count:.0f}")
    LOG.info(f"Null count: {nulls:,.0f} ({nulls/count:.3%}")
    LOG.info(f"0% count: {zeros:,.0f} ({zeros/count:.3%})")
    LOG.info(f"100% count: {ones:,.0f} ({ones/count:.3%})")
    LOG.info(f"Other count: {other:,.0f} ({other/count:.3%})")


def plot_bare_soil_distribution(vals: pd.Series, name: str = "", assumption: float = None):
    sns.set_theme(
        context="talk",
        style="whitegrid",
        palette=sns.color_palette("husl", 1),
    )
    fig, ax = plt.subplots(figsize=(16, 8))
    sns.histplot(x=vals, stat="proportion", ax=ax, bins=25, color="#00A33B")
    ax.xaxis.set_major_formatter(PercentFormatter(xmax=1))
    ax.yaxis.set_major_formatter(PercentFormatter(xmax=1))
    ax.set_xlabel("Proportion of bare soil")
    ax.set_ylabel("Proportion of parcels")
    mean = vals.mean()
    # median = vals.median()
    ax.axvline(x=mean, color="black", ls="--")
    ax.annotate(f"Mean: {mean:.0%}", xy=(mean + 0.01, 0.05))
    if assumption is not None:
        ax.axvline(x=assumption, color="black", ls="-.")
        ax.annotate(
            f"Current assumption\nfor payment rate: {assumption:.0%}", xy=(assumption + 0.01, 0.05)
        )

    xpos = 0.05
    nulls = vals.isnull().sum()
    count = vals.size
    name += " "
    fig.suptitle(
        f"Distribution of {name} by bare soil cover", x=xpos, y=0.92, ha="left", fontsize="large"
    )
    fig.supxlabel(
        "Source: Sentinel-2 L2A imagery, November 2021 - February 2022.\n"
        f"No data for {nulls:,.0f} of {count:,.0f} fields ({nulls/count:.3%}) "
        "due to cloud cover in the imagery",
        x=xpos,
        y=-0.05,
        ha="left",
        fontsize="small",
        wrap=True,
    )
    sns.despine(top=True, right=True, left=True, bottom=True)
    fig.show()


def calc_bare_soil_percent_parcel(geometry: Polygon, da: DataArray) -> float:
    """Calculate the % bare soil in a parcel given it's geometry and a an array of ndvi data
    Parameters:
        geometry: A shapley polygon of the parcel geometry - must be in asame CRS as `da`
        da: A binary data array reprasenting bare soil
    Returns:
        A float reprasenting the mean of the pixels in the parcel geometry.
            Returns `np.nan` if the parcel is outside the bounds of the image
    """
    try:
        # Clipping by the bounding box before clipping speeds this up a lot!
        # da_clip = da.rio.clip_box(*geometry.bounds).rio.clip([geometry])
        # mean = float(np.mean(da_clip, where=da_clip!=da_clip.rio.nodata)) / 100
        mean = float(da.rio.clip_box(*geometry.bounds).rio.clip([geometry]).mean())
    except OneDimensionalRaster:
        # Where parcels are very small we need higher resolution or will fail...
        LOG.warning("One of the parcel dimensions is very small, setting value to Nan")
        # The below blows up memory use to c. 90GB - need to use integers...
        # da_high_res = da.rio.reproject(dst_crs=da.rio.crs, resolution=1) #1m
        # mean = float(da_high_res.rio.clip_box(*geometry.bounds).rio.
        # clip([geometry], from_disk=from_disk).mean())
        mean = np.nan
    except NoDataInBounds:
        LOG.warning("Parcel outside of tile bounds, setting value to Nan")
        mean = np.nan
    return mean


def calc_bare_soil_percent(
    path_ndvi: str,
    ndvi_thresh: float = 0.25,
    resolution: Optional[float] = None,
    simplify: Optional[float] = None,
    batch_size: int = 500,
):
    """Calculate the % bare soil for some parcel geometries
    Parameters:
        path_ndvi: Path to the NDVI raster file
        ndvi_thresh: The Normalised Difference Vegetation Index (NDVI) threshold below
        which the pixel is classed as bare soil.
        resolution: The raster resolution to reproject to. Defaults to 10m which is the
        maximum resolution available fromt the Sentinel 2A data. Increasing this will speed up
        the computation but is likely to affect the result.
        simplify: The tolerance to simplify the geometry to in meters. By default no
        simplification is performed but this can speed things up for needlessly complex geometries
        batch_size: The maximum number of rows per batch - used to set the value of
            `spark.sql.execution.arrow.maxRecordsPerBatch`
    Returns:
        A `pandas_udf` iterator function which splits a pandas Series of WKB parcel
            geometries into batches for processing
    Note:
        This uses global variable path_ndvi to decide which raster to use,
    """
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(batch_size))

    @F.pandas_udf("float")
    def _calc_bare_soil_percent(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
        da = rxr.open_rasterio(path_ndvi).squeeze(drop=True)
        if resolution is not None:
            if resolution <= 10:
                raise ValueError(
                    "resolution must be a number higher than 10 in metres. "
                    "Value provided: {resolution}."
                )
            da = da.rio.reproject(dst_crs=da.rio.crs, resolution=resolution)
        # TODO: Think this is overwriting nan which is not intended! Check!
        da.data = xr.where(da < ndvi_thresh, 1.0, 0.0, keep_attrs=True)
        for series in iterator:
            # raster EPSG will be 32630 or 32631 depending on the timezone
            geoms = gpd.GeoSeries.from_wkb(series, crs=27700).to_crs(epsg=da.rio.crs.to_epsg())
            if simplify is not None:
                geoms = geoms.simplify(tolerance=simplify)
            # clip here to remove unwanted regions e.g. sea where possible
            # TODO: Buffer - account for hedges - check effect in changes in buffering number [0,1]
            da_clipped = da.rio.clip_box(*geoms.total_bounds)
            yield geoms.map(lambda x: calc_bare_soil_percent_parcel(x, da=da_clipped))

    return _calc_bare_soil_percent
