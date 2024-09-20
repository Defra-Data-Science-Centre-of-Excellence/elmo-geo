"""Ordnance Survey Datasets, provided by DASH.
- National Geographic Database, is the single store of all Ordnance Survey's authoritative data.
- Open GreenSpace
- MasterMap Highways Network Roads Prem

[^os]: https://www.ordnancesurvey.co.uk/products/os-ngd
[^dash]: https://app.powerbi.com/Redirect?action=OpenReport&appId=5762de14-3aa8-4a83-92b3-045cc953e30c&reportObjectId=c8802134-4f3b-484e-bf14-1ed9f8881450&ctid=770a2450-0227-4c62-90c7-4e38537f1102&reportPage=ReportSectionff2a0c223272005d9b10&pbi_source=appShareLink&portalSessionId=f7a19b52-4676-43dd-9f13-d1084081a8f2
"""

from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl.etl import SRID, DerivedDataset, SourceDataset, SourceGlobDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels


class OSNGDRaw(DataFrameModel):
    """Model for merged OS NGD datasets.

    Attributes:
        fid: is the row id.
        osid: Ordnance Survey Identifier is the primary identifier and unique key for this theme.
        toid: Topographic Identifier is an additional secondary identifier which can aid further data linking.
        geometry: Any type geometries in EPSG:27700.
    """

    fid: int = Field(unique=True)
    osid: str = Field(unique=True)
    toid: str = Field()
    theme: str = Field()
    description: str = Field()
    layer: str = Field()
    geometry: Geometry(crs=SRID) = Field()


os_ngd_raw = SourceGlobDataset(
    name="os_ngd_raw",
    level0="bronze",
    level1="os",
    model=OSNGDRaw,
    restricted=True,
    glob_path="/dbfs/mnt/base/restricted/source_ordnance_survey_data_hub/dataset_ngd*/format_GPKG*/LATEST_*/*.gpkg",
)


class OSBNGModel(DataFrameModel):
    """OS British National Grid grid reference
    dataset data model.

    Attributes:
        tile_name: Name of the grid reference tile.
        layer: Resolution of the grid reference tile. Either
            1km_grid, 5km_grid, 10km_grid, 20km_grid, 50km_grid,
            100km_grid.
        geometry: Geometry of the tile.
    """

    tile_name: str = Field()
    layer: str = Field()
    geometry: Geometry = Field()


os_bng_raw = SourceDataset(
    name="os_bng_raw",
    level0="bronze",
    level1="os",
    restricted=False,
    source_path="/dbfs/mnt/lab/unrestricted/ELM-Project/raw/os_bng_grids.gpkg",
    model=OSBNGModel,
    partition_cols=["layer"],
)
"""OS British National Grid grid reference geometries."""


class OSBNGParcelsModel(DataFrameModel):
    """OS British National Grid grid reference joined to parcels
    data model.

    Attributes:
        id_parcel: RPA reference parcels ID
        tile_name: Name of the grid reference tile.
        layer: Resolution of the grid reference tile. Either
            1km_grid, 5km_grid, 10km_grid, 20km_grid, 50km_grid,
            100km_grid.
        proportion: Proportion of the parcel intersected by the tile.
    """

    id_parcel: str = Field()
    tile_name: str = Field()
    layer: str = Field()
    proportion: float = Field(ge=0, le=1)


os_bng_parcels = DerivedDataset(
    name="os_bng_parcels",
    level0="silver",
    level1="os",
    restricted=False,
    is_geo=False,
    model=OSBNGParcelsModel,
    func=partial(join_parcels, columns=["layer", "tile_name"]),
    dependencies=[reference_parcels, os_bng_raw],
    partition_cols=["layer"],
)
"""OS British National Grid grid reference geometries joined to parcels."""
