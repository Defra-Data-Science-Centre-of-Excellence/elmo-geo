"""Peatland datasets from Defra, supplied by DASH.

Peaty Soils is a simplified England Peat Status GHG and C storage (Peat Layer).
Peat Layer contains many more features and columns than Peaty Soils, but they are the same total area to 1ha.

[^DASH: Peaty Soils]: https://app.powerbi.com/groups/de0d7293-1d23-4194-869d-a4ff2ed2d169/reports/c8802134-4f3b-484e-bf14-1ed9f8881450?ctid=770a2450-0227-4c62-90c7-4e38537f1102&pbi_source=linkShare&bookmarkGuid=093de660-dc9d-4bf4-bf82-deeb3aa69dc6
[^DASH: Search "peat"]: https://app.powerbi.com/groups/de0d7293-1d23-4194-869d-a4ff2ed2d169/reports/c8802134-4f3b-484e-bf14-1ed9f8881450?ctid=770a2450-0227-4c62-90c7-4e38537f1102&pbi_source=linkShare&bookmarkGuid=efe914ce-17ad-44c4-b3d5-21a5f07e7929
"""
from functools import partial

from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
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
    geometry: Geometry(crs=SRID) = Field(coerce=True)


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
        proportion: proportion of Parcel Geometry(crs=SRID) overlapping with feature geometry.
    """

    id_parcel: str = Field()
    group: str = Field()
    proportion: float = Field(ge=0, le=1)


peaty_soils_parcels = DerivedDataset(
    is_geo=False,
    name="peaty_soils_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    func=partial(join_parcels, columns=["group"]),
    dependencies=[reference_parcels, peaty_soils_raw],
    model=PeatySoilsParcels,
)
