"""Priority Habitats Inventory (PHI).

The [Priority Habitats Inventory](https://www.data.gov.uk/dataset/4b6ddab7-6c0f-4407-946e-d6499f19fcde/priority-habitats-inventory-england) 
is a spatial dataset indicating the locations and extents of habitat of 'principle importance'.

The data is not a exhaustive survey of habitat locations but is the best available indication of the locations of specific habitat types.
"""
from functools import partial, reduce

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry
from pyspark.sql import functions as F

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import sjoin_parcel_proportion
from elmo_geo.st.join import sjoin
from elmo_geo.st.udf import st_clean
from elmo_geo.utils.types import SparkDataFrame

from .rpa_reference_parcels import reference_parcels


def split_mainhabs(sdf: SparkDataFrame) -> SparkDataFrame:
    """Splits the mainhabs field into a row for each habitat type.

    Creates new habitat_name column for the split habitats
    and drops the original column.
    """
    return sdf.withColumn("habitat_name", F.expr("EXPLODE(SPLIT(mainhabs, ','))")).drop("mainhabs")


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
    fid: str = Field(unique=False, alias="uid")
    geometry: Geometry(crs=SRID) = Field()


class PriorityHabitatParcels(DataFrameModel):
    """Model describing the Defra Priority Habitats data join to RPA parcels.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        fid: Unique identifier for habitat geometry. Because each geometry can be labelled with multiple
            habitats the fid is retained in the aprcels join to ensure correct aggregation of proportions
            in derived datasets.
        habitat_name: The name of the priority habitat.
        proportion: The proportion of the parcel that intersects with the spatial priority.
    """

    id_parcel: str = Field()
    fid: str = Field()
    habitat_name: str = Field()
    proportion: float = Field(ge=0, le=1)


defra_priority_habitat_england_raw = SourceDataset(
    name="defra_priority_habitat_england_raw",
    medallion="bronze",
    source="defra",
    restricted=False,
    clean_geometry=True,  # Splits geometries resulting in duplciate fids.
    partition_cols=["habcodes"],
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_priority_habitats_inventory_eng/format_GEOPARQUET_priority_habitats_inventory_eng/LATEST_priority_habitats_inventory_eng/ne_priority_habitat_inventory_england.parquet",
    model=PHIEnglandRawModel,
)
"""Definition for Defra Priority Habitats source data."""

defra_priority_habitat_parcels = DerivedDataset(
    name="defra_priority_habitat_parcels",
    medallion="silver",
    source="defra",
    restricted=False,
    is_geo=False,
    func=partial(sjoin_parcel_proportion, columns=["fid", "habitat_name"], fn_pre=split_mainhabs),
    dependencies=[reference_parcels, defra_priority_habitat_england_raw],
    model=PriorityHabitatParcels,
)
"""Definition for Defra Priority Habitats joined to RPA Parcels."""


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
    sdf_phi = priority_habitats_raw.sdf().withColumn("geometry", F.expr("ST_SubDivideExplode(geometry, 256)")).transform(st_clean).transform(split_mainhabs)

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
    medallion="silver",
    source="defra",
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
