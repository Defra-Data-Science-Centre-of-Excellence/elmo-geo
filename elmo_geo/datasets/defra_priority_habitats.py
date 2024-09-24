"""Priority Habitats Inventory (PHI).

The [Priority Habitats Inventory](https://www.data.gov.uk/dataset/4b6ddab7-6c0f-4407-946e-d6499f19fcde/priority-habitats-inventory-england) 
is a spatial dataset indicating the locations and extents of habitat of 'principle importance'.

The data is not a exhaustive survey of habitat locations but is the best available indication of the locations of specific habitat types.
"""
from functools import partial, reduce

import pandas as pd
from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry
from pyspark.sql import functions as F

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels
from elmo_geo.st.geometry import load_geometry
from elmo_geo.st.join import knn, sjoin
from elmo_geo.utils.types import SparkDataFrame

from .rpa_reference_parcels import reference_parcels

DISTANCE_THRESHOLD = 5_000


def split_mainhabs(sdf: SparkDataFrame) -> SparkDataFrame:
    """Splits the mainhabs field into a row for each habitat type.

    Creates new habitat_name column for the split habitats
    and drops the original column.
    """
    return sdf.withColumn("habitat_name", F.expr("EXPLODE(SPLIT(mainhabs, ','))")).drop("mainhabs")


_join_parcels = partial(join_parcels, columns=["habitat_name"], fn_pre=split_mainhabs)


def _habitat_proximity(parcels: Dataset, habitats: Dataset, habitat_filter_expr: str, max_vertices: int = 256) -> pd.DataFrame:
    """Calculate the distance from each parcel to each selected habitat type.

    Used to find the closest habitat to each parcel and the associated distance. Applies filter
    to habitats dataset so that resulting proximity dataset is for a subset of priority habitats.

    Imposes distance threshold, preset to 5km, to avoid costly distance calculations.

    Parameters:
        parcels: The parcels dataset.
        habitats: The habitats dataset.
        habitat_filter_exr: SQL expression for filtering the habitats dataset.
        max_vertices: Max number of habitat geometries vertices. Geometries exceeding this threshold are split.

    Returns:
        DataFrame of parcel id to nearest habitat and the associated distance.
    """
    sdf_habitat = (
        habitats.sdf()
        .transform(split_mainhabs)
        .filter(F.expr(habitat_filter_expr))
        .select("habitat_name", "geometry")
        .withColumn("geometry", F.expr(f"ST_SubdivideExplode(geometry, {max_vertices})"))
    )

    sdf_parcels = parcels.sdf().select("id_parcel", "geometry")

    sdf_knn = knn(sdf_parcels, sdf_habitat, id_left="id_parcel", id_right="habitat_name", k=1, distance_threshold=DISTANCE_THRESHOLD).drop("rank")

    # Aggregate to return dataset with single row per parcel, closest habitat and corresponding distance.
    return sdf_knn.join(sdf_knn.groupBy("id_parcel").agg(F.min("distance").alias("distance")), on=["id_parcel", "distance"], how="inner").toPandas()


_heathland_habitat_proximity = partial(_habitat_proximity, habitat_filter_expr="habitat_name like '%heath%'")
_grassland_habitat_proximity = partial(
    _habitat_proximity,
    habitat_filter_expr="habitat_name in ('{}')".format(
        "','".join(
            [
                "Lowland calcareous grassland",
                "Upland calcareous grassland",
                "Lowland dry acid grassland",
                "Lowland meadows",
                "Purple moor grass and rush pastures",
                "Grass moorland",
            ]
        )
    ),
)


class PHIEnglandRawModel(DataFrameModel):
    """Data model for all England source PHI dataset.

    Attributes:
        mainhabs: Habitat names corresponding to the geometry, concatenated together
            separated by a comma.
        habcodes: Habitata type code
        areaha: Habitat area in hectares
        fid: Identifier for habitat
        geometry: Habitat geometry
    """

    mainhabs: str = Field()
    habcodes: str = Field()
    areaha: float = Field()
    version: str = Field()
    fid: str = Field(unique=True, alias="uid")
    geometry: Geometry(crs=SRID) = Field()


class PriorityHabitatParcels(DataFrameModel):
    """Model describing the Defra Priority Habitats data join to RPA parcels.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        habitat_name: The name of the priority habitat.
        proportion: The proportion of the parcel that intersects with the spatial priority.
    """

    id_parcel: str = Field()
    habitat_name: str = Field()
    proportion: float = Field(ge=0, le=1)


class PriorityHabitatProximity(DataFrameModel):
    """Model describing the distance between habitats in the
    Defra Priority Habitats dataset and RPA parcels.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        habitat_name: The name of the priority habitat.
        distance: The distance from the parcel to this type of habitat.
    """

    id_parcel: str = Field()
    habitat_name: str = Field()
    distance: int = Field()


defra_priority_habitat_england_raw = SourceDataset(
    name="defra_priority_habitat_england_raw",
    level0="bronze",
    level1="defra",
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_priority_habitats_inventory_eng/format_GEOPARQUET_priority_habitats_inventory_eng/LATEST_priority_habitats_inventory_eng/ne_priority_habitat_inventory_england.parquet",
    model=PHIEnglandRawModel,
)
"""Definition for Defra Priority Habitats source data."""

defra_priority_habitat_parcels = DerivedDataset(
    name="defra_priority_habitat_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    is_geo=False,
    func=_join_parcels,
    dependencies=[reference_parcels, defra_priority_habitat_england_raw],
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
    dependencies=[reference_parcels, defra_priority_habitat_england_raw],
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
    dependencies=[reference_parcels, defra_priority_habitat_england_raw],
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


def _habitat_area_within_distance(
    sdf_parcels: SparkDataFrame,
    sdf_phi: SparkDataFrame,
    distance_threshold: int,
) -> SparkDataFrame:
    """Performs distance join between parcels and priority habitats and sums the habitat area per parcel."""
    return (
        sjoin(sdf_parcels, sdf_phi, distance=distance_threshold)
        .withColumn("distance", F.expr("ST_Distance(geometry_left, geometry_right)"))
        .groupby("id_parcel", "habitat_name")
        .agg(
            F.expr("SUM(ST_Area(geometry_right)) AS area"),
            F.expr("CAST(ROUND(MIN(distance),0) AS int) AS minimum_distance"),
        )
        .withColumn("distance_threshold", F.lit(distance_threshold))
    )


def _habitat_area_within_distances(
    parcels: Dataset,
    priority_habitats_raw: Dataset,
    distances: list[int] = [1_000, 2_000, 3_000, 5_000],
) -> SparkDataFrame:
    """Calculates the area of priority habitat within each threshold distance to parcels."""
    sdf_phi = (
        priority_habitats_raw.sdf()
        .withColumn("geometry", load_geometry(encoding_fn=""))
        .withColumn("geometry", F.expr("ST_SubDivideExplode(geometry, 256)"))
        .transform(split_mainhabs)
    )

    def union(sdf1: SparkDataFrame, sdf2: SparkDataFrame) -> SparkDataFrame:
        return sdf1.unionByName(sdf2, allowMissingColumns=False)

    return reduce(union, [_habitat_area_within_distance(parcels.sdf(), sdf_phi, distance) for distance in distances])


class PriorityHabitatArea(DataFrameModel):
    """Model describing the area of priority habitats at different
    threshold distances from RPA parcels.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        habitat_name: The name of the priority habitat.
        area: The area of priority habitat geometries that are within the threshold distance, in m2. The area
        of the whole geometry is given, even if only part of the geometry is within the threshold.
        minimum_distance: Minimum distance to this type of habitat.
        distance_threshold: The threshold distance in metres.
    """

    id_parcel: str = Field(coerce=True)
    habitat_name: str = Field(coerce=True)
    area: float = Field(coerce=True)
    minimum_distance: int = Field(coerce=True)
    distance_threshold: int = Field(coerce=True)


defra_habitat_area_parcels = DerivedDataset(
    name="defra_habitat_area_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    is_geo=False,
    func=_habitat_area_within_distances,
    dependencies=[reference_parcels, defra_priority_habitat_england_raw],
    model=PriorityHabitatArea,
)
"""Area of Defra Priority Habitats within 2km and 5km of a parcel.

Used, in combination with other datasets, to classify which types of habitats can be created
on parcels as part of ELM habitat creation actions.

Two thresholds of 2km and 5km are used to allow for different criteria for rarer habitats.
At a single 5km threshold we expect rarer priority habitats to have smaller areas that other
habitat types, which could overly bias habitat creation classification towards more common
habitats. Using a second 2km threshold allows checking for nearby rarer habitats before moving to
more common habitats. 
"""
