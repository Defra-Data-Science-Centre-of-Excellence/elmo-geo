import shutil
import tempfile
from typing import Dict, Set, Union

import matplotlib.pyplot as plt
import numpy as np
import rasterio
import seaborn as sns
import xarray as xr
from xarray.core.dataarray import DataArray

from elmo_geo import LOG


def write_array_to_raster(arr, filename, **meta):
    """Save an array of data to a .tif format raster file.

    Parameters:
        arr: (array-like) – This may be a numpy.ma.MaskedArray.
        filename: (str) Path to save the file to.
        meta: Metadata passed to the rasterio.open() function
        when creating a raster file writer.

    Returns:
        None
    """
    with tempfile.NamedTemporaryFile(suffix=".tif") as tmp:
        with rasterio.open(tmp.name, mode="w", **meta) as dst:
            dst.write(arr)
        shutil.copy(tmp.name, filename)


def to_raster(da: DataArray, path: str):
    """Save a DataArray to dbfs e.g. .tif
    temp file then move it...
    """
    filename = path.split("/")[-1]
    temp_loc = f"/tmp/{filename}"
    da.rio.to_raster(temp_loc)
    shutil.move(temp_loc, path)
    LOG.info(f"Saved DataArray to {path}")


def get_raster_info(rst: DataArray, plot: bool = True):
    """Log and plot raster information
    Parameters:
        rst: THe raster band
        plot: Whether to plot the data and its histogram or just log info
    """
    LOG.info(f"CRS: {rst.rio.crs}")
    LOG.info(f"Extent: {rst.rio.bounds()}")
    LOG.info(f"Resolution: {rst.rio.resolution()}")
    LOG.info(f"Shape: {rst.shape}")
    LOG.info(f"Min: {np.nanmin(rst.data)}")
    LOG.info(f"Max: {np.nanmax(rst.data)}")
    LOG.info(f"Mean: {np.nanmean(rst.data)}")
    LOG.info(f"Median: {np.nanmedian(rst.data)}")
    LOG.info(f"Var: {np.nanvar(rst.data)}")
    LOG.info(f"Std: {np.nanstd(rst.data)}")
    LOG.info(f"Nans: {np.isnan(rst.data).sum()}")
    LOG.info(f"Nans %: {np.isnan(rst.data).sum()/np.size(rst.data):%}")
    LOG.info(f"No data value: {rst.rio.nodata}")
    if plot:
        sns.set(style="whitegrid")
        rst.plot()
        plt.show()
        rst.plot.hist(bins=50, log=True)
        plt.show()


def trim_band(band: DataArray, val: int, na_val=np.nan) -> DataArray:
    """Set values in a band above val to na_val"""
    LOG.info(f"Setting any value less than `{val}` to `{na_val}`")
    return xr.where(band < val, band, na_val, keep_attrs=True).rio.write_crs(band.rio.crs)


def set_nodata(band: DataArray, val: Union[int, float], nodataval=np.nan) -> DataArray:
    """Set val values in a band to na_val"""
    LOG.info(f"Setting no data `{val}` values to `{nodataval}`")

    if val != nodataval:
        band.data = xr.where(band == val, nodataval, band, keep_attrs=True)
    return band.rio.write_nodata(nodataval)


def _check_bands(bands: Dict[str, DataArray], required_bands: Set[str]):
    bands_present = set(bands.keys())
    assert required_bands.issubset(bands_present), f"Could not find required bands {required_bands} in {bands_present}"
    for band1 in required_bands:
        for band2 in required_bands:
            assert bands[band1].rio.crs == bands[band2].rio.crs, "CRS mismatch between {band1} and {band2}"
            assert bands[band1].rio.bounds() == bands[band2].rio.bounds(), "Bounds mismatch between {band1} and {band2}"
            assert bands[band1].rio.resolution() == bands[band2].rio.resolution(), "Resolution mismatch between {band1} and {band2}"


def normalised_diff(a: DataArray, b: DataArray) -> DataArray:
    """Calculate the normalised difference between two bands
    e.g. for NDVI, NDSI
    Parameters:
        a: The first band
        b: The second band
    Returns:
        The array of normalised difference values between -1 and 1
    """
    return (a - b) / (a + b)


def summarise_cloud_cover(ra: DataArray):
    """Log summary stats for certain and uncertain cloud cover"""
    valid_pixels = float(ra.count())
    def_clouds = float((ra == 1.0).sum())
    not_clouds = float((ra == 0.0).sum())
    uncertain = valid_pixels - (def_clouds + not_clouds)
    LOG.info(f"Certain cloud cover: {def_clouds/valid_pixels:.2%}")
    LOG.info(f"Uncertain cloud cover: {uncertain/valid_pixels:.2%}")


def apply_offset(da: DataArray, offset: int, floor0: bool = True) -> DataArray:
    """Apply an offset to an array e.g. to reverse radiometric offsets in Sentinel products
    Parameters:
        da: The array of integer values
        offset: The integer offset
        floor0: Whether to set negative values to 0
    Returns:
        The datarray with the offset applied
    """
    if offset == 0:
        return da
    da = da + offset  # apply the offset
    if floor0:
        da.data = xr.where(da < 0, 0, da, keep_attrs=True)  # set negative values to 0
    return da
