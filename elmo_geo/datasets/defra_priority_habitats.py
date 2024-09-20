"""Priority Habitats Inventory (PHI).

The [Priority Habitats Inventory](https://www.data.gov.uk/dataset/4b6ddab7-6c0f-4407-946e-d6499f19fcde/priority-habitats-inventory-england) 
is a spatial dataset indicating the locations and extents of habitat of 'principle importance'.

The data is not a exhaustive survey of habitat locations but is the best available indication of the locations of specific habitat types.
"""
from functools import partial

import pandas as pd
from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.pandas_engine import Geometry
from pyspark.sql import functions as F

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import sjoin_parcel_proportion
from elmo_geo.st.join import knn
from elmo_geo.utils.types import SparkDataFrame

from .rpa_reference_parcels import reference_parcels

DISTANCE_THRESHOLD = 5_000


def split_mainhabs(sdf: SparkDataFrame) -> SparkDataFrame:
    """Splits the mainhabs field into a row for each habitat type.

    Creates new habitat_name column for the split habitats
    and drops the original column.
    """
    return sdf.withColumn("habitat_name", F.expr("EXPLODE(SPLIT(mainhabs, ','))")).drop("mainhabs")


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
    habitat_name: Category = Field()
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
    habitat_name: Category = Field()
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
    func=partial(sjoin_parcel_proportion, columns=["habitat_name"], fn_pre=split_mainhabs),
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
