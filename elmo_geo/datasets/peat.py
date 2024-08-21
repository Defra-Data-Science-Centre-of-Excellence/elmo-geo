"""Peatland datasets from Defra, supplied by DASH.

Peaty Soils is a simplified England Peat Status GHG and C storage (Peat Layer).
Peat Layer contains many more features and columns than Peaty Soils, but they are the same total area to 1ha.

[^DASH: Search "peat"]:
"""
from functools import partial

from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels


class PeatySoilsRaw(DataFrameModel):
    """Model for Defra Peaty Soils (2008) dataset.

    Attributes:
        fid: feature id.
        group: peatland class description:
            Deep Peaty Soils
            Shallow Peaty Soils
            Soils with Peaty Pockets
        geometry: Polygon geometries in EPSG:27700.
    """

    fid: int = Field(coerce=True, unique=True, alias="objectid")
    group: Category = Field(coerce=True, alias="pclassdesc", isin=["Deep Peaty Soils", "Shallow Peaty Soils", "Soils with Peaty Pockets"])
    geometry: Geometry = Field(coerce=True)


peaty_soils_raw = SourceDataset(
    name="peaty_soils_raw",
    level0="bronze",
    level1="defra",
    model=PeatySoilsRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_peaty_soils/format_GEOPARQUET_peaty_soils/LATEST_peaty_soils/refdata_owner/",
)


class PeatySoilsParcels(DataFrameModel):
    """Model for Defra Peaty Soils with parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        group: peatland class description.
        proportion: proportion of Parcel geometry overlapping with feature geometry.
    """

    id_parcel: str = Field()
    group: bool = Field()
    proportion: float = Field(ge=0, le=1)


peaty_soils_parcels = DerivedDataset(
    name="peaty_soils_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    func=partial(join_parcels, columns=["group"]),
    dependencies=[reference_parcels, peaty_soils_raw],
    model=PeatySoilsParcels,
)
