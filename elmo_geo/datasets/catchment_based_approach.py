"""Data from the Catchment Based Approach Data Hub.

The CaBA Data Hub [^1] is a set of spatial data sources suitable for supporting
integrated catchment management planning. The datasets are displayed, interpreted
and grouped to help partnerships identify issues and opportunities for
collaborative action.
"""

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import sjoin_parcel_proportion

from .rpa_reference_parcels import reference_parcels


class WetlandVisionRaw(DataFrameModel):
    """Model for raw version of Wetland Vision Future Potential Wetlands.

    Parameters:
        geometry: Geometry indicating the potential extent of wetlands.
    """

    geometry: Geometry(crs=SRID) = Field(coerce=True)


wetland_vision_raw = SourceDataset(
    name="wetland_vision_raw",
    level0="bronze",
    level1="catchment_based_approach",
    model=WetlandVisionRaw,
    restricted=False,
    is_geo=True,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/catchment_based_approach_data_hub/wetland_vision_future_potential_wetlands/Wetland_Vision_Future_Potential_Wetlands.shp",
)
"""Wetland Vision Future Potential Wetlands raw dataset.

 Source:
    - https://data.catchmentbasedapproach.org/datasets/theriverstrust::wetland-vision-future-potential-wetlands/about
"""


class WetlandVisionParcels(DataFrameModel):
    """Model for Defra ALC with parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        proportion: proportion of Parcel geometry overlapping with feature geometry.
    """

    id_parcel: str = Field()
    proportion: float = Field(ge=0, le=1)


wetland_vision_parcels = DerivedDataset(
    is_geo=False,
    name="wetland_vision_parcels",
    level0="silver",
    level1="catchment_based_approach",
    restricted=False,
    func=sjoin_parcel_proportion,
    dependencies=[reference_parcels, wetland_vision_raw],
    model=WetlandVisionParcels,
)
