"""Protected Landscape datasets, supplied by DASH.

Protected Landscapes include; National Parks, and National Landscapes (previously known as Areas of Outstanding Natural Beauty (AONBs)).


[^DASH: AONB]:
[^DASH: National Parks]:
"""
from functools import partial

from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import combine, join_parcels

from .rpa_reference_parcels import reference_parcels


class NationalParksRaw(DataFrameModel):
    """Model for National Parks dataset.

    Attributes:
        name: Name of the National Park.
        geometry: Polygon geometries in EPSG:27700.
    """

    name: str = Field(coerce=True)
    geometry: Geometry = Field(coerce=True)


national_parks_raw = SourceDataset(
    name="national_parks_raw",
    level0="bronze",
    level1="defra",
    model=NationalParksRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_parks/format_GEOPARQUET_national_parks/LATEST_national_parks/",
)


class NationalLandscapesRaw(DataFrameModel):
    """Model for National Landscape dataset.
    Previously known as Areas of Outstanding Natural Beauty (AONBs).

    Attributes:
        name: Name of the National Landscape.
        geometry: (Multi)Polygon geometries in EPSG:27700.
    """

    name: str = Field(coerce=True)
    geometry: Geometry = Field(coerce=True)


national_landscapes_raw = SourceDataset(
    name="national_landscapes_raw",
    level0="bronze",
    level1="defra",
    model=NationalLandscapesRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_areas_of_outstanding_natural_beauty/format_GEOPARQUET_areas_of_outstanding_natural_beauty/LATEST_areas_of_outstanding_natural_beauty",
)


class ProtectedLandscapesTidy(DataFrameModel):
    """Model for a combined Protected Landscapes dataset.

    Attributes:
        source: Protected Landscape is a National Park or National Landscape.
        name: Name of the protected landscape.
        geometry: (Multi)Polygon geometries in EPSG:27700.
    """

    source: Category = Field(isin=["National Park", "National Landscape"])
    name: str = Field()
    proportion: float = Field(ge=0, le=1)

protected_landscapes_tidy = DerivedDataset(
    name="protected_landscapes_tidy",
    level0="silver",
    level1="defra",
    restricted=False,
    func=partial(combine, sources=["National Park", "National Landscape"]),
    dependencies=[national_parks_raw, national_landscapes_raw],
    model=ProtectedLandscapesTidy,
)


class ProtectedLandscapesParcel(DataFrameModel):
    """Model for Protected Landscapes with parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        source: Is this area conclusively common land, or only suggested by historic data sources.
        proportion: proportion of Parcel geometry overlapping with feature geometry.
    """

    id_parcel: str = Field()
    conclusive: bool = Field()
    proportion: float = Field(ge=0, le=1)

protected_landscapes_parcels = DerivedDataset(
    name="protected_landscapes_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    func=partial(join_parcels, columns=["source"]),
    dependencies=[reference_parcels, protected_landscapes_tidy],
    model=ProtectedLandscapesParcel,
)
