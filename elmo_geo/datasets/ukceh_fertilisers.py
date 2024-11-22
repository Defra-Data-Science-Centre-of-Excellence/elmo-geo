from elmo_geo.etl import DerivedDataset, SourceSingleFileRasterDataset
from elmo_geo.etl.transformations import get_centroid_value_from_raster
from elmo_geo.rs.raster import interp_nearest

from .rpa_reference_parcels import reference_parcels



import geopandas as gpd
from shapely import Geometry
from typing import Callable
import pyspark.sql.functions as F
import pandas as pd

import numpy as np
from xarray.core.dataarray import DataArray
from os import PathLike
from elmo_geo.etl import SourceSingleFileRasterDataset

ukceh_nitrogen_raw = SourceSingleFileRasterDataset(
    name="ukceh_nitrogen_raw",
    level0="bronze",
    level1="ukceh",
    restricted=True,
    source_path="/dbfs/mnt/lab/restricted/ukceh/land_cover_plus_fertilisers_2010_2015/data/fertiliser_n_prediction_uncertainty.tif",
)
"""Dataset definition for UKCEH nitrogen fertiliser application data.

This dataset has 2 bands, the first is the nitrogen prediction in tonnes/km2, the second is the uncertainty in the same units."""

def _calculate_nitrogen(ukceh_nitrogen_raw: SourceSingleFileRasterDataset, reference_parcels: DerivedDataset) -> gpd.GeoDataFrame:
    """Lookup the UKCEH nitrogen value for each parcel in tonnes/km2, interpolating missing values from the nearest ones."""
    sdf = reference_parcels.sdf().select("id_parcel", "geometry").withColumn("geom_wkb", F.expr("ST_AsEWKB(geometry)"))
    return (
        sdf
        .withColumn(
            "nitrogen",
            get_centroid_value_from_raster(
                raster_dataset=ukceh_nitrogen_raw,
                raster_processing=lambda ra: interp_nearest(ra.sel(band=1)),
            )(sdf.geom_wkb))
        .drop("geom_wkb", "geometry")
        .toPandas()
    )

ukceh_nitrogen_parcels = DerivedDataset(
    name="ukceh_nitrogen_parcels",
    level0="silver",
    level1="ukceh",
    restricted=True,
    is_geo=False,
    func=_calculate_nitrogen,
    dependencies=[ukceh_nitrogen_raw, reference_parcels],
)

