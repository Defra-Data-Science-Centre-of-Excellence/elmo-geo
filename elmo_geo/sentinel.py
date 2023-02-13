import glob
import os
import re
import shutil
import zipfile
from typing import List, Optional, Tuple

import geopandas as gpd
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import seaborn as sns
from bs4 import BeautifulSoup
from matplotlib.ticker import PercentFormatter

from elmo_geo.log import LOG


def get_tile(filename: str) -> str:
    """Extract tile name from Sentinel filename
    Parameters:
        filename: The
    Returns:
        The 5-character tile name, e.g. 29UPA
    """
    regex = r"(?!.*R[0-9]{3}_T)([0-9]{2}[A-Z]{3})(?=_[0-9]{8})"
    return re.search(regex, filename).group(0)


def extract_sentinel_data(filename: str):
    """Extract a zipped Sentinel file from FileStore to
    /mnt/lab/unrestricted/sentinel/
    Parameters:
        filename: Name of the zip file
    """
    FROM = "/dbfs/FileStore/"
    TO = "/dbfs/mnt/lab/unrestricted/sentinel/"
    LOG.info(f"Moving {filename} from {FROM} to {TO}")
    shutil.move(FROM + filename, TO + filename)
    LOG.info(f"Extracting {filename}")
    with zipfile.ZipFile(TO + filename, "r") as zip_ref:
        zip_ref.extractall(TO)
    LOG.info("Deleting zip file")
    os.remove(TO + filename)
    LOG.info("Done!")


def extract_all_sentinel_data(path: Optional[str] = None):
    """Extract all zipped Sentinel files
    Parameters:
        path: The directory to search for downloaded files, defaults to
            `/dbfs/mnt/lab/unrestricted/sentinel`
    """
    if path is None:
        path = "/dbfs/mnt/lab/unrestricted/sentinel"
    files = [f for f in os.listdir(path) if f.endswith(".zip")]
    LOG.info(f"Found {len(files):,.0f} zip files in {path} to extract")
    if len(files) == 0:
        LOG.info("Aborting")
        return
    LOG.info("Extracting files...")
    for filename in files:
        LOG.info(f"Unzipping {filename}")
        with zipfile.ZipFile(path + "/" + filename, "r") as zip_ref:
            zip_ref.extractall(path)
        LOG.info("Removing the zip file")
        os.remove(path + "/" + filename)
    LOG.info("Done!")


def is_downloaded(filename: str, path: Optional[str] = None):
    """Check if a sentinel produce has been downloaded
    Parameters:
        filename: The name of the file
        path: The directory to search for downloaded files, defaults to
            `/dbfs/mnt/lab/unrestricted/sentinel`
    Returns:
        True or False
    """
    if path is None:
        path = "/dbfs/mnt/lab/unrestricted/sentinel"
    return filename in os.listdir(path)


def find_sentinel_data(
    root_dir: str = None,
    mission: str = None,
    product: str = None,
    year: int = None,
    month: int = None,
    day: int = None,
    hour: int = None,
    minute: int = None,
    second: int = None,
    pdgs: str = None,
    orbit: str = None,
    tile: str = None,
    discrim: str = None,
) -> List[str]:
    """Search a directory for sentinel data and return a list of matches
    Parameters:
        root_dir: The directory to search through
        mission: Three characters reprasenting the mission (e.g. `S2A` for Sentinel 2a)
        product: Five characters reprasenting the product, (e.g. `MSIL2A` for Sentinel 2a)
        year: The year the image was captured
        month: The month the image was captured
        day: The day the image was captured
        hour: The hour the image was captured (24-hour)
        minute: The minute the image was captured
        second: The second the image was captured
        pdgs: The PDGS processing baseline number (e.g. N0204)
        orbit: The relative orbit number (R001 - R143)
        tile: The tile number
        discrim: A 15-character date string (the Product Discriminator) used to distinguish between
            different end user products from the same datatake. Depending on the instance, the time
            in this field can be earlier or slightly later than the datatake sensing time.
    Returns:
        A list of datasets in the folder that match the criteria (full paths)
    Note:
        See https://sentinel.esa.int/web/sentinel/user-guides/sentinel-2-msi/naming-convention
            for more information on the naming conventions.
    """

    DEFAULT_LOCATION = "/dbfs/mnt/lab/unrestricted/sentinel"
    PATH_STR = (
        ".*{mission}_{product}_{year}{month}{day}T{hour}{minute}{second}"
        "_{pdgs}_{orbit}_{tile}_{discrim}.SAFE"
    )

    if root_dir is None:
        root_dir = DEFAULT_LOCATION

    if mission is None:
        mission = r".{3}"

    if product is None:
        product = r".{6}"

    if year is None:
        year = r"[0-9]{4}"
    else:
        year = str(year).rjust(4, "0")

    if month is None:
        month = r"[0-9]{2}"
    else:
        month = str(month).rjust(2, "0")

    if day is None:
        day = r"[0-9]{2}"
    else:
        day = str(day).rjust(2, "0")

    if hour is None:
        hour = r"[0-9]{2}"
    else:
        hour = str(hour).rjust(2, "0")

    if minute is None:
        minute = r"[0-9]{2}"
    else:
        minute = str(minute).rjust(2, "0")

    if second is None:
        second = r"[0-9]{2}"
    else:
        second = str(second).rjust(2, "0")

    if pdgs is None:
        pdgs = r".{5}"

    if orbit is None:
        orbit = r".{4}"

    if tile is None:
        tile = r".{6}"

    if discrim is None:
        discrim = r".{15}"

    regex = PATH_STR.format(
        mission=mission,
        product=product,
        year=year,
        month=month,
        day=day,
        hour=hour,
        minute=minute,
        second=second,
        pdgs=pdgs,
        orbit=orbit,
        tile=tile,
        discrim=discrim,
    )
    regex = re.compile(regex)
    files = os.listdir(root_dir)
    result = [x for x in files if regex.match(x) is not None]
    LOG.info(f"Found the following datasets: {result}")
    return [f"{root_dir}/{x}" for x in result]


def find_sentinel_qi_data(data_path: str, name: str) -> str:
    """Search a sentinel data directory for bands at a given resolution
    Parameters:
        data_path: The data directory to search for the bands (e.g. that returned from a
            `find_sentinel_data()` call)
        name: The required data name (e.g. MSK_CLDPRB_20m, MSK_CLDPRB_60m)
    Returns:
        The path of the required jp2 file
    Raises:
        FileNotFoundError: When no file with a name of data is found
    """
    resolution_dirs = glob.glob(f"{data_path}/GRANULE/*/QI_DATA/{name}.jp2")
    try:
        return next(iter(resolution_dirs))
    except StopIteration:
        raise FileNotFoundError(
            f"Could not find {name}.jp2 in the QI_DATA directory of {data_path}"
        )


def find_sentinel_bands(
    data_path: str, resolution: int, band: str = None, type: Optional[str] = None
) -> str:
    """Search a sentinel data directory for bands at a given resolution
    Parameters:
        data_path: The data directory to search for the bands (e.g. that returned from a
            `find_sentinel_data()` call)
        resolution: The resolution in metres (e.g. 10, 20, 60).
        band: The required band (e.g. B1, B2, B3, B4, B5, B6, B7, B8, B8A, B9, B10, B11,
            B12, AOT, TIC, WVP, SCL). Regex may be used to subset the bands.
    Returns:
        A list of bands in the folder that match the criteria (full paths)
    """
    if band is None:
        band = r"[0-9]{2}"
    resolution_dirs = glob.glob(f"{data_path}/GRANULE/*/IMG_DATA/*")
    regex = re.compile(f"{data_path}/GRANULE/.*/IMG_DATA/R{resolution}m".replace("/", r"\/"))
    resolution_dir = next(x for x in resolution_dirs if regex.match(x) is not None)
    regex = re.compile(f".*{band}_{resolution}m.jp2")
    files = os.listdir(resolution_dir)
    result = [x for x in files if regex.match(x) is not None]
    LOG.info(f"Found the following bands: {result}")
    return [f"{resolution_dir}/{x}" for x in result]


def get_winter_datasets(yearend: int, tile: str) -> List[str]:
    """Get a list of dataset paths for a given winter and tile
    Parameters:
        yearend: The year the winter ends in
        tile: The tile code, e.g. `T30UWC`
    Returns:
        A list of paths to the dataset folders
    """
    dates = [(yearend - 1, 11), (yearend - 1, 12), (yearend, 1), (yearend, 2)]
    datasets = [find_sentinel_data(product="MSIL2A", year=y, month=m, tile=tile) for y, m in dates]
    datasets = [d for sublist in datasets for d in sublist]  # flatten the list
    LOG.info(f"Found {len(datasets)} datasets for tile {tile}")
    return datasets


def get_image_usefulness(path: str) -> float:
    """Return an estimate for the % usefull pixels for bare soils analysis in a dataset
    Usefulness is defined as the sum of vegetation and bare soil as a % of total pixels.
    It is read from the dataset's metadata in the `MTD_MSIL2A.xml` file
    Parameters:
        path: full path to the dataset folder
    Returns:
        The proportion of the image that is estimated to have bare soil or vegetation
    """
    with open(path + "/MTD_MSIL2A.xml", "r") as f:
        data = f.read()
    data = BeautifulSoup(data, "xml")
    vegetation = float(data.find("VEGETATION_PERCENTAGE").contents[0])
    bare_soil = float(data.find("NOT_VEGETATED_PERCENTAGE").contents[0])
    usefulness = (vegetation + bare_soil) / 100
    LOG.info(f"Dataset {path} has a usefulness of {usefulness:.2%}")
    return usefulness


def get_image_radiometric_offset(path: str, band: str) -> float:
    """Return the radiometric offset for the image band so that it can be reversed.
    A radimetric offset of -1000 was applied to PB04.00 products (post Jan 25th 2022).
    This offset results in lower NDVI values and so we want to reverse it to align with previous
    data.
    Parameters:
        path: full path to the dataset folder
        band: a string reprasenting the band in question
    Returns:
        The integer offset applied to the reflectance values
    """
    with open(path + "/MTD_MSIL2A.xml", "r") as f:
        data = f.read()
    band = str(int(re.findall(r"\d+", "B08")[0]))
    tag = BeautifulSoup(data, "xml").find("BOA_ADD_OFFSET", band_id=band)
    if tag is None:
        return 0
    return int(tag.contents[0])


def sort_datasets_by_usefulness(datasets: List[str]) -> List[str]:
    """Sort the dataset paths by decending usefulness
    Parameters:
        datasets: A list of full paths to dataset folders
    Returns:
        The list fo datasets sorted by decending usefulness.

    Usefulness is the proportion that is estimated to have bare soil or vegetation
    """
    usefulness_dict = {d: get_image_usefulness(d) for d in datasets}
    return [x[0] for x in sorted(usefulness_dict.items(), key=lambda x: x[1], reverse=True)]


def plot_products(
    df: gpd.GeoDataFrame, title: str, show_nmax: int = 0
) -> Tuple[plt.Figure, plt.Axes]:
    """Plot a view of products within a timeframe
    Parameters:
        df: A dataframe of Sentinel 2A products with `beginposition`
            column and the derived `usefulpercentage` column
        title: Title to show on the plot
        show_nmax: Whether to highlight the top n useful datasets
    Returns:
        The matplotlib figure and axes to plot
    """
    sns.set_style("white")
    sns.set_context("notebook")

    fig, ax = plt.subplots(figsize=(20, 6))
    fig.suptitle(title, x=0.12, ha="left", fontsize="xx-large")
    fig.supxlabel(
        "Note: Usefulness is defined as the sum of estimated vegetation and"
        "bare ground cover in %.",
        x=0.12,
        ha="left",
    )
    ax.plot(
        df.beginposition,
        df.usefulpercentage,
        ms=8,
        marker="o",
        mew=1,
        mec="w",
        lw=1,
        c="grey",
        zorder=0,
        label="Product",
    )

    if show_nmax:
        df_useful = df.loc[df.useful]
        ax.scatter(
            df_useful.beginposition,
            df_useful.usefulpercentage,
            s=250,
            marker="*",
            linewidths=1,
            edgecolor="w",
            c="#007DBA",
            label="Useful",
        )
        top_n_mean = df.loc[
            df.usefulpercentage.rank(ascending=False) <= show_nmax, "usefulpercentage"
        ].mean()
        top_n_max = df.loc[
            df.usefulpercentage.rank(ascending=False) <= show_nmax, "usefulpercentage"
        ].max()
        top_n_min = df.loc[
            df.usefulpercentage.rank(ascending=False) <= show_nmax, "usefulpercentage"
        ].min()
        ax.axhline(top_n_max, color="#007DBA", lw=1, alpha=0.3, zorder=0)
        ax.axhline(top_n_mean, ls="--", color="black", lw=1, zorder=0)
        ax.axhline(top_n_min, color="#007DBA", lw=1, alpha=0.3, zorder=0)
        ax.annotate(
            f"Max: {top_n_max/100:.0%}",
            (df.beginposition.min(), top_n_max),
            xytext=(2, 2),
            textcoords="offset pixels",
        )
        ax.annotate(
            f"Mean usefulness of top {show_nmax} products: {top_n_mean/100:.0%}",
            (df.beginposition.min(), top_n_mean),
            xytext=(2, 2),
            textcoords="offset pixels",
        )
        ax.annotate(
            f"Min: {top_n_min/100:.0%}",
            (df.beginposition.min(), top_n_min),
            xytext=(2, 2),
            textcoords="offset pixels",
        )
        ax.fill_between(
            [df.beginposition.min(), df.beginposition.max()],
            top_n_max,
            top_n_min,
            color="#007DBA",
            alpha=0.1,
            zorder=0,
        )

    df_downloaded = df.loc[df.downloaded]
    ax.scatter(
        df_downloaded.beginposition,
        df_downloaded.usefulpercentage,
        s=100,
        marker="s",
        linewidths=1,
        edgecolor="w",
        c="#00A33B",
        label="Downloaded",
    )
    ax.legend()
    locator = mdates.AutoDateLocator(minticks=12, maxticks=40)
    formatter = mdates.ConciseDateFormatter(locator)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    ax.set_xlim(df.beginposition.min(), df.beginposition.max())
    ax.yaxis.set_major_formatter(PercentFormatter(xmax=100))
    ax.set_ylabel("Useful pixels")
    ax.set_xlabel("Image date")
    sns.despine(left=True, top=True, right=True, bottom=True)
    return fig, ax
