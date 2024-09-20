"""National Character Areas (NCA) from Defra, provided by DASH.

This describes the geological land types.

[^DASH: NCA]: https://app.powerbi.com/groups/de0d7293-1d23-4194-869d-a4ff2ed2d169/reports/c8802134-4f3b-484e-bf14-1ed9f8881450?ctid=770a2450-0227-4c62-90c7-4e38537f1102&pbi_source=linkShare&bookmarkGuid=a13d092b-8842-4f99-85c6-8620172a94bc
"""
from functools import partial

from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels


class NCARaw(DataFrameModel):
    """Model for Defra National Character Areas (NCA) dataset.

    Attributes:
        jcaname: Joint Character Area Name (e.g. Northumberland Sandstone Hills)
        nca_name: National Character Area Name (e.g. Northumberland Sandstone Hills)
        naname: Natural Area Name (e.g. Border Uplands)
        hotlink: URL to NCA profile
        alt: Agricultural Landscape Typology
            Eastern Arable, Upland Fringe, Upland, Chalk and Limestone Mixed, SE Mixed (Wooded), Western mixed, Western Mixed, Unclassified, None
        blt: Broad Landscape Typology
            Low-lying coast, Sandstone hills and ridges, Upland fringe, Upland, Moorland and moorland fringe, Vales and valleys, Lowlands, Magnesian limestone,
            Limestone hills, Chalk wolds and downs, Estuary, "Fens, levels and marshes", Coal measures, Forests and parklands, Conurbation, Limestone wolds,
            Rugged coast, Lowland heath, Claylands, None
        geometry: NCA geometries in EPSG:27700.
    """

    jcaname: str = Field(nullable=True)
    nca_name: str = Field(nullable=True)
    naname: str = Field(nullable=True)
    hotlink: str = Field(nullable=True)
    alt: Category = Field(nullable=True)
    blt: Category = Field(nullable=True)
    geometry: Geometry(crs=SRID) = Field()


nca_raw = SourceDataset(
    name="nca_raw",
    level0="bronze",
    level1="defra",
    model=NCARaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_character_areas/format_GEOPARQUET_national_character_areas/LATEST_national_character_areas/",
)


class NCAParcels(DataFrameModel):
    """Model for Defra NCA with parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        blt: Broad Landscape Typology
        proportion: proportion of Parcel geometry overlapping with feature geometry.
    """

    id_parcel: str = Field()
    blt: str = Field(nullable=True)
    proportion: float = Field(ge=0, le=1)


nca_parcels = DerivedDataset(
    is_geo=False,
    name="nca_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    func=partial(join_parcels, columns=["blt"]),
    dependencies=[reference_parcels, nca_raw],
    model=NCAParcels,
)
