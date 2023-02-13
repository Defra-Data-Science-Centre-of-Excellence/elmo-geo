from typing import Callable, List

import numpy as np
import rioxarray as rxr
import xarray as xr

from elmo_geo.log import LOG
from elmo_geo.raster import (
    apply_offset,
    normalised_diff,
    set_nodata,
    summarise_cloud_cover,
)
from elmo_geo.sentinel import (
    find_sentinel_bands,
    find_sentinel_qi_data,
    get_image_radiometric_offset,
    sort_datasets_by_usefulness,
)


def process_ndvi_cloud_prob(dataset: str, inc_tci: bool = False) -> xr.Dataset:
    """Read in required bands and calculate NDVI and cloud probability
    Parameters:
        dataset: The path of the dataset directory
        inc_tci: Whether or not to include the true color image `tci` - used for
            validation but is slower
    Returns:
        A dataset with arrays for `ndvi`, `cloud_prob`, and sometimes `tci`.
    """

    LOG.info(f"Processing: {dataset}")

    # preprocess bands
    required_bands = [
        {"name": "red", "resolution": 10, "band": "B04"},  # for NDVI
        {"name": "nir", "resolution": 10, "band": "B08"},  # for NDVI
    ]
    return_vars = ["ndvi", "cloud_prob"]
    if inc_tci:
        required_bands.append({"name": "tci", "resolution": 10, "band": "TCI"})
        return_vars.append("tci")

    band_paths = {
        b["name"]: find_sentinel_bands(
            dataset,
            resolution=b["resolution"],
            band=b["band"],
        )[0]
        for b in required_bands
    }
    band_paths["cloud_prob"] = find_sentinel_qi_data(dataset, "MSK_CLDPRB_60m")
    # read the bands into a dict
    ds = {k: rxr.open_rasterio(v).squeeze() for k, v in band_paths.items()}

    # adjust for radiometric offsets
    for band in required_bands:
        offset = get_image_radiometric_offset(dataset, band["band"])
        ds[band["name"]] = apply_offset(ds[band["name"]], offset)

    # reproject 60m and 20m bands to 10m resolution
    ds["cloud_prob"] = ds["cloud_prob"].rio.reproject_match(ds["red"])
    ds["cloud_prob"] = ds["cloud_prob"].astype("float64") / 100.0  # cloud prob to float 0-1
    ds = dict(
        (k, set_nodata(v, 0).astype("float64") / 10000.0)
        if k not in ("tci", "cloud_prob")
        else (k, v)
        for k, v in ds.items()
    )

    # cloud_prob is impossible to separate nodata from 0% so need to get from another band e.g. red
    ds["cloud_prob"].data = xr.where(ds["red"].isnull(), np.nan, ds["cloud_prob"], keep_attrs=True)
    ds["cloud_prob"] = set_nodata(ds["cloud_prob"], np.nan)
    ds = xr.Dataset(data_vars=ds)

    # Calc NDVI
    ds["ndvi"] = normalised_diff(ds["nir"], ds["red"])
    summarise_cloud_cover(ds["cloud_prob"])
    return ds[return_vars]


def replace_ndvi_cloud_prob(ds: xr.Dataset, ds_new: xr.Dataset) -> xr.Dataset:
    ds = xr.where(
        (ds["cloud_prob"] > ds_new["cloud_prob"])  # where there more clouds
        | ds["cloud_prob"].isnull(),  # or where there is no data
        ds_new,  # replace the pixels with the new dataset
        ds,  # otherwise leave them
        keep_attrs=True,
    )
    summarise_cloud_cover(ds["cloud_prob"])
    return ds


def finally_ndvi_cloud_prob(ds: xr.Dataset) -> xr.Dataset:
    CLOUD_PROB_THRESHOLD = 0.2  # remaining pixels with cloud prob above this will be np.nan
    remaining_clouds = float(
        xr.where(ds["cloud_prob"] > CLOUD_PROB_THRESHOLD, 1, 0).sum() / ds["cloud_prob"].size
    )
    LOG.info(f"Remaining clouds: {remaining_clouds:.2%}")
    ds["ndvi"] = xr.where(
        ds["cloud_prob"] > CLOUD_PROB_THRESHOLD, np.nan, ds["ndvi"], keep_attrs=True
    )
    return ds


def get_clean_image(
    datasets: List[str],
    process_func: Callable[[str], xr.Dataset],
    replace_func: Callable[[xr.Dataset, xr.Dataset], xr.Dataset],
    finally_func: Callable[[xr.Dataset], xr.Dataset],
) -> xr.Dataset:
    """Generate a clean dataset for a tile/granule by backfilling other datasets
    Requires injection of three dependant functions to process each dataset,
    replace pixels from one with the next, and do some final clean up.
    Parameters:
        datasets: A list of paths to downloaded Sentinel granules
        process_func: The function to read in each dataset from the path and process it
        replace_func: The function with logic to replace some pixels in the first
            dataset with pixels from the next
        finally_func: The function to do any final actions
    Returns:
        A dataset with arrays for `ndvi`, and potentially other metrics such as
            `cloud_prob`, and `tci` depending on the injected functions.
    """

    # sort by decending order of usefulness
    datasets = sort_datasets_by_usefulness(datasets)
    iter_datasets = iter(datasets)
    # Process the first dataset
    ds = process_func(next(iter_datasets))
    crs = ds.rio.crs

    for dataset in iter_datasets:
        # Process the next dataset
        ds_new = process_func(dataset)
        # Replace selected pixels from ds with those from ds_new
        ds = replace_func(ds, ds_new)

    # Finally tidy up and summarise
    ds = finally_func(ds)
    return ds.rio.write_crs(crs)
