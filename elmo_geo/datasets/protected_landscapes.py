"""Protected Landscape datasets, supplied by DASH.

Protected Landscapes include; National Parks, and National Landscapes (previously known as Areas of Outstanding Natural Beauty (AONBs)).


[^DASH: AONB]: https://app.powerbi.com/groups/de0d7293-1d23-4194-869d-a4ff2ed2d169/reports/c8802134-4f3b-484e-bf14-1ed9f8881450?ctid=770a2450-0227-4c62-90c7-4e38537f1102&pbi_source=linkShare&bookmarkGuid=8ecba55a-b8f9-4f4b-ab28-c5ec816d1eca
[^DASH: National Parks]: https://app.powerbi.com/groups/de0d7293-1d23-4194-869d-a4ff2ed2d169/reports/c8802134-4f3b-484e-bf14-1ed9f8881450?ctid=770a2450-0227-4c62-90c7-4e38537f1102&pbi_source=linkShare&bookmarkGuid=b8f1bc02-8b8b-4b2f-8b4d-e46c535fd4cc
"""
from functools import partial

from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import combine, join_parcels

from .rpa_reference_parcels import reference_parcels


class NationalParksRaw(DataFrameModel):
    """Model for National Parks dataset.

    Attributes:
        name: Name of the National Park.
        geometry: Polygon geometries in EPSG:27700.
    """

    name: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


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
    geometry: Geometry(crs=SRID) = Field(coerce=True)


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
    is_geo=False,
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
        proportion: proportion of Parcel Geometry(crs=SRID) overlapping with feature geometry.
    """

    id_parcel: str = Field()
    conclusive: bool = Field()
    proportion: float = Field(ge=0, le=1)


protected_landscapes_parcels = DerivedDataset(
    is_geo=False,
    name="protected_landscapes_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    func=partial(join_parcels, columns=["source"]),
    dependencies=[reference_parcels, protected_landscapes_tidy],
    model=ProtectedLandscapesParcel,
)
