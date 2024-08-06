"""Defra Priority Habitats.

The [Priority Habitats Inventory](https://www.data.gov.uk/dataset/4b6ddab7-6c0f-4407-946e-d6499f19fcde/priority-habitats-inventory-england) 
is a spatial dataset indicating the locations and extents of habitat of 'principle importance'.

The data is not a exhaustive survey of habitat locations but is the best available indication of the locations of specific habitat types.
"""
from functools import partial

import pandas as pd
from pandera import DataFrameModel, Field
from pandera.dtypes import Category, Date
from pandera.engines.pandas_engine import Geometry
from pyspark.sql import functions as F

from elmo_geo.etl import Dataset, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels
from elmo_geo.st.geometry import clean_geometry
from elmo_geo.st.join import knn
from elmo_geo.utils.types import SparkDataFrame

from .rpa_reference_parcels import reference_parcels

DISTANCE_THRESHOLD = 5_000
_join_parcels = partial(join_parcels, columns=["Main_Habit"])


def _combine(south: Dataset, central: Dataset, north: Dataset) -> pd.DataFrame:
    """Union the priority habitats datasets for different regions into a single dataset."""
    return pd.concat([south.gdf(), central.gdf(), north.gdf()])


def _habitat_proximity(
    parcels: Dataset, habitats: Dataset, habitat_filter_expr: str, simplify_tolerence: float = 20.0, max_vertices: int = 256
) -> SparkDataFrame:
    """Calculate the distance from each parcel to each selected habitat type.

    Used to find the closest habitat to each parcel and the associated distance. Applies filter
    to habitats dataset so that resulting proximity dataset is for a subset of priority habitats.

    Imposes distance threshold, preset to 5km, to avoid costly distance calculations.

    Parameters:
        parcels: The parcels dataset.
        habitats: The habitats dataset.
        habitat_filter_exr: SQL expression for filtering the habitats dataset.
        simplify_tolerence: Tolerance to use when simplifying geometries.
        max_vertices: Max number of habitat geometries veritices. Geoometries exceeding this threshol are split.

    Returns:
        SparkDataFrame of parel id to nearest habitat and the associated distance.
    """
    sdf_habitat = (
        habitats.sdf()
        .filter(F.expr(habitat_filter_expr))
        .select("Main_Habit", "geometry")
        .withColumn("geometry", clean_geometry("geometry", simplify_tolerence=simplify_tolerence))
        .withColumn("geometry", F.expr(f"ST_SubdivideExplode(geometry, {max_vertices})"))
        .repartition(1_000)  # approx 1,600 records per partition
    )

    sdf_parcels = (
        parcels.sdf()
        .select("id_parcel", "geometry")
        .withColumn("geometry", clean_geometry("geometry", simplify_tolerence=simplify_tolerence))
        .repartition(1_000)
    )

    sdf_knn = knn(sdf_parcels, sdf_habitat, id_left="id_parcel", id_right="Main_Habit", k=1, distance_threshold=DISTANCE_THRESHOLD).drop("rank")

    # Aggregate to return dataset with single row per parcel, closest habitat and corresponding distance.
    return sdf_knn.join(sdf_knn.groupBy("id_parcel").agg(F.min("distance").alias("distance")), on=["id_parcel", "distance"], how="inner").toPandas()


_heathland_habitat_proximity = partial(_habitat_proximity, habitat_filter_expr="Main_Habit like '%heath%'")
_grassland_habitat_proximity = partial(
    _habitat_proximity,
    habitat_filter_expr="Main_Habit in ('Lowland calcareous grassland','Upland calcareous grassland',"
    "'Lowland dry acid grassland','Lowland meadows',"
    "'Purple moor grass and rush pastures','Grass moorland')",
)


class DefraPriorityHabitatsRaw(DataFrameModel):
    """Model describing the Defra's Priority Habitats dataset.

    Parameters:
        geometry: The sensitivity classification's geospatial extent (polygons).
    """

    Main_Habit: str = Field(nullable=False)
    Confidence: str = Field(nullable=True)
    Source1: str = Field(nullable=True)
    S1Date: str = Field(nullable=True, coerce=True)
    S1Habclass: str = Field(nullable=True)
    S1HabType: str = Field(nullable=True)
    Source2: str = Field(nullable=True)
    S2Date: str = Field(nullable=True, coerce=True)
    S2Habclass: str = Field(nullable=True)
    S2HabType: str = Field(nullable=True)
    Source3: str = Field(nullable=True)
    S3Date: str = Field(nullable=True, coerce=True)
    S3Habclass: str = Field(nullable=True)
    S3HabType: str = Field(nullable=True)
    Basemappng: str = Field(nullable=True)
    Annex_1: str = Field(nullable=True)
    Add_habits: str = Field(nullable=True)
    Candidates: str = Field(nullable=True)
    Rule_Decis: str = Field(nullable=True)
    GenComment: str = Field(nullable=True)
    LastModDat: Date = Field(nullable=True, coerce=True)
    ModReason: str = Field(nullable=True)
    Mod_by: str = Field(nullable=True)
    Area_Ha: float = Field(nullable=False)
    URN: str = Field(nullable=False)
    geometry: Geometry = Field(coerce=True, nullable=False)


class PriorityHabitatParcels(DataFrameModel):
    """Model describing the Defra Priority Habitats data join to RPA parcels.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        Main_Habit: The name of the priority habitat.
        proportion: The proportion of the parcel that intersects with the spatial priority.
    """

    id_parcel: str
    Main_Habit: Category = Field(coerce=True)
    proportion: float = Field(ge=0, le=1)


class PriorityHabitatProximity(DataFrameModel):
    """Model describing the distance between habitats in the
    Defra Priority Habitats dataset and RPA parcels.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        Main_Habit: The name of the priority habitat.
        distance: The distance from the parcel to this type of habitat.
    """

    id_parcel: str
    Main_Habit: Category = Field(coerce=True)
    distance: int = Field(coerce=True)


defra_priority_habitat_raw_south = SourceDataset(
    name="defra_priority_habitat_raw_south",
    level0="bronze",
    level1="defra",
    restricted=False,
    is_geo=True,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_priority_habitat_inventory_south/format_GEOPARQUET_priority_habitat_inventory_south/LATEST_priority_habitat_inventory_south/layer=PHI_v2_3_South.snappy.parquet",
)
"""Raw version of the priority habitats data - South England. 

Source:
    - [Defra Priority Habitats](https://www.data.gov.uk/dataset/4b6ddab7-6c0f-4407-946e-d6499f19fcde/priority-habitats-inventory-england)
"""

defra_priority_habitat_raw_central = SourceDataset(
    name="defra_priority_habitat_raw_central",
    level0="bronze",
    level1="defra",
    restricted=False,
    is_geo=True,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_priority_habitat_inventory_central/format_GEOPARQUET_priority_habitat_inventory_central/LATEST_priority_habitat_inventory_central/layer=PHI_v2_3_Central.snappy.parquet",
)
"""Raw version of the priority habitats data - Central England. 

Source:
    - [Defra Priority Habitats](https://www.data.gov.uk/dataset/4b6ddab7-6c0f-4407-946e-d6499f19fcde/priority-habitats-inventory-england)
"""

defra_priority_habitat_raw_north = SourceDataset(
    name="defra_priority_habitat_raw_north",
    level0="bronze",
    level1="defra",
    restricted=False,
    is_geo=True,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_priority_habitat_inventory_north/format_GEOPARQUET_priority_habitat_inventory_north/LATEST_priority_habitat_inventory_north/layer=PHI_v2_3_North.snappy.parquet",
)
"""Raw version of the priority habitats data - North England. 

Source:
    - [Defra Priority Habitats](https://www.data.gov.uk/dataset/4b6ddab7-6c0f-4407-946e-d6499f19fcde/priority-habitats-inventory-england)
"""

defra_priority_habitat_england = DerivedDataset(
    name="defra_priority_habitat_england",
    level0="silver",
    level1="defra",
    restricted=False,
    func=_combine,
    dependencies=[defra_priority_habitat_raw_south, defra_priority_habitat_raw_central, defra_priority_habitat_raw_north],
    model=DefraPriorityHabitatsRaw,
)
"""Definition for Defra Priority Habitats joined to RPA Parcels."""

defra_priority_habitat_parcels = DerivedDataset(
    name="defra_priority_habitat_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    is_geo=False,
    func=_join_parcels,
    dependencies=[reference_parcels, defra_priority_habitat_england],
    model=PriorityHabitatParcels,
)
"""Definition for Defra Priority Habitats joined to RPA Parcels."""


defra_heathland_proximity_parcels = DerivedDataset(
    name="defra_heathland_proximity_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    is_geo=False,
    func=_heathland_habitat_proximity,
    dependencies=[reference_parcels, defra_priority_habitat_england],
    model=PriorityHabitatProximity,
)
"""Defra Priority Habitats distance from parcels to heathland habitats.
"""

defra_grassland_proximity_parcels = DerivedDataset(
    name="defra_grassland_proximity_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    is_geo=False,
    func=_grassland_habitat_proximity,
    dependencies=[reference_parcels, defra_priority_habitat_england],
    model=PriorityHabitatProximity,
)
"""Defra Priority Habitats distance from parcels to grassland habitats.

The following grassland habitats are included:
- Lowland calcareous grassland
- Upland calcareous grassland
- Lowland dry acid grassland
- Lowland meadows
- Purple moor grass and rush pastures
- Grass moorland
"""
