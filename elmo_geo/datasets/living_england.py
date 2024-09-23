"""Living England - Habitat Map (Phase 4), provided by DASH.

Note: Comes in 3 parquet parts, and such reading the folder combines them.

[^DASH: Living England]: https://app.powerbi.com/groups/de0d7293-1d23-4194-869d-a4ff2ed2d169/reports/c8802134-4f3b-484e-bf14-1ed9f8881450?ctid=770a2450-0227-4c62-90c7-4e38537f1102&pbi_source=linkShare&bookmarkGuid=9d1245cf-f40f-437f-9040-66c401566f11
"""

from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels


class LivingEnglandHabitatMapPhase4Raw(DataFrameModel):
    """Model for Living England's Habitat Map (Phase 4) dataset.

    Habitats:
        Acid, Calcareous, Neutral Grassland
        Arable and Horticultural
        Bare Ground
        Bare Sand
        Bog
        Bracken
        Broadleaved, Mixed and Yew Woodland
        Built-up Areas and Gardens
        Coastal Saltmarsh
        Coastal Sand Dunes
        Coniferous Woodland
        Dwarf Shrub Heath
        Fen, Marsh and Swamp
        Improved Grassland
        Scrub
        Water
        Unclassified

    Attributes:
        A_pred: Name of the most likely habitats (comma separated Habitats).
        A_prob: Probability percentage of A_pred.
        B_pred: Name of the second most likely habitat.
        B_prob: Probability percentage of B_pred.
        geometry: (Multi)Polygon geometries in EPSG:27700.
    """

    A_pred: str = Field()
    A_prob: float = Field(ge=0, le=100)
    B_pred: str = Field(nullable=True)
    B_prob: float = Field(nullable=True, ge=0, le=100)
    geometry: Geometry(crs=SRID) = Field()


living_england_habitat_map_phase_4_raw = SourceDataset(
    name="living_england_habitat_map_phase_4_raw",
    level0="bronze",
    level1="living_england",
    model=LivingEnglandHabitatMapPhase4Raw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_living_england_habitat_map_phase_4/format_GEOPARQUET_living_england_habitat_map_phase_4/LATEST_living_england_habitat_map_phase_4/",
)


class LivingEnglandHabitatMapPhase4Parcel(DataFrameModel):
    """Model for Habitat Map joined to parcels.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        A_pred: Name of the most likely habitats.
        proportion: proportion of Parcel Geometry(crs=SRID) overlapping with feature geometry.
    """

    id_parcel: str = Field()
    A_pred: str = Field()
    proportion: float = Field(ge=0, le=1)


living_england_habitat_map_phase_4_parcels = DerivedDataset(
    is_geo=False,
    name="living_england_habitat_map_phase_4_parcel",
    level0="silver",
    level1="living_england",
    restricted=False,
    func=partial(join_parcels, columns=["A_pred"]),
    dependencies=[reference_parcels, living_england_habitat_map_phase_4_raw],
    model=LivingEnglandHabitatMapPhase4Parcel,
)
