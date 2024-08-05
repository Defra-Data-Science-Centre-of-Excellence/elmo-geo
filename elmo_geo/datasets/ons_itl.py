"""International Territorial Levels (ITLs) from Office of National Satitstics (ONS).

Data taken from open geography portal here:
https://geoportal.statistics.gov.uk/datasets/6750ae0351c749c4b40b31e5740233a0_0/explore?location=54.959130%2C-3.316600%2C6.04 """

from functools import partial

import geopandas as gpd
from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, SourceDataset, Dataset, DerivedDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels

_join_parcels = partial(join_parcels, columns=["ITL221NM"])

class ITL2Boundaries(DataFrameModel):
    """Model for ONS ITL2 (counties and groups of counties) dataset.

    Parameters:
        ITL221CD: Reference unique id for each geographic area ie TLC1.
        ITL221NM: Name of the county or group of counties ie Tees Valley and Durham
        geometry: The ITL geospatial polygons are in EPSG:27700.
    """

    ITL221CD: str = Field(coerce=True)
    ITL221NM: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)

class ITL2BoundariesParcels(DataFrameModel):
    """Model for ONS ITL2 with parcel dataset.

    Parameters:
        ITL221CD: Reference unique id for each geographic area ie TLC1.
        ITL221NM: Name of the county or group of counties ie Tees Valley and Durham
        geometry: The ITL geospatial polygons are in EPSG:27700.
        proportion: The proportion of rpa refernce parcels that are within with each of the itl2 boundary.
    """

    ITL221CD: str = Field(coerce=True)
    ITL221NM: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)
    Proportion: float = Field(ge=0, le=1)


itl2_boundaries = SourceDataset(
    name="itl2_boundaries",
    level0="silver",
    level1="ons",
    model=ITL2Boundaries,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_int_territorial_lvl2_2021_uk_bgc_v2/format_GPKG_int_territorial_lvl2_2021_uk_bgc_v2/LATEST_int_territorial_lvl2_2021_uk_bgc_v2/International_Territorial_Level_2_January_2021_UK_BGC_V2_2022_1205324512979248673.gpkg",
)

itl2_boundaries_parcels = DerivedDataset(
    name="itl2_boundaries_parcels",
    level0="silver",
    level1="ons",
    restrictes=False,
    func=_join_parcels,
    dependencies=[reference_parcels, itl2_boundaries],
    model=ITL2BoundariesParcels
)
