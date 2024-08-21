"""Living England - Habitat Map (Phase 4), provided by DASH.

Note: Comes in 3 parquet parts, and such reading the folder combines them.

[^DASH: Living England]: TODO
"""

from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import DerivedDataset, SourceDataset
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
        A_pred: Name of the most likely habitat.
        A_prob: Probability percentage of A_pred.
        B_pred: Name of the second most likely habitat.
        B_prob: Probability percentage of B_pred.
        geometry: (Multi)Polygon geometries in EPSG:27700.
    """

    A_pred: str = Field(coerce=True)
    A_prob: float = Field(coerce=True, ge=0, le=100)
    B_pred: str = Field(coerce=True)
    B_prob: float = Field(coerce=True, ge=0, le=100)
    geometry: Geometry = Field(coerce=True)


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
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        A_pred: The most likely habitat.
        proportion: proportion of Parcel geometry overlapping with feature geometry.
    """

    id_parcel: str = Field()
    A_pred: bool = Field()
    proportion: float = Field(ge=0, le=1)


living_england_habitat_map_phase_4_parcel = DerivedDataset(
    name="living_england_habitat_map_phase_4_parcel",
    level0="silver",
    level1="living_england",
    restricted=False,
    func=partial(join_parcels, columns=["A_pred"]),
    dependencies=[reference_parcels, living_england_habitat_map_phase_4_raw],
    model=LivingEnglandHabitatMapPhase4Parcel,
)
