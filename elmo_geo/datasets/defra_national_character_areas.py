"""National Character Areas (NCA) from Defra, provided by DASH.

This describes the geological land types.

[^DASH: NCA]:
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

    jcaname: str = Field(coerce=True, nullable=True, alias="Joint Character Area Name")
    nca_name: str = Field(coerce=True, nullable=True, alias="National Character Area Name")
    naname: str = Field(coerce=True, nullable=True, alias="Natural Area Name")
    hotlink: str = Field(coerce=True, nullable=True)
    alt: Category = Field(coerce=True, nullable=True, alias="Agricultural Landscape Typology")
    blt: Category = Field(coerce=True, nullable=True, alias="Broad Landscape Typology")
    geometry: Geometry(crs=SRID) = Field(coerce=True)


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
        geometry: NCA geometries in EPSG:27700.
    """

    id_parcel: str = Field()
    blt: Category = Field()
    proportion: float = Field(ge=0, le=1)


nca_parcels = DerivedDataset(
    name="nca_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    func=partial(join_parcels, columns=["blt"]),
    dependencies=[reference_parcels, nca_raw],
    model=NCAParcels,
)
