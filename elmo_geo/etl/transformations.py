"""Functions for transforming datasets.

For use in `elmo.etl.DerivedDataset.func`.
"""
import pandas as pd
from pyspark.sql import functions as F

from elmo_geo.st.geometry import load_geometry
from elmo_geo.st.join import sjoin
from elmo_geo.utils.types import SparkDataFrame

from .etl import Dataset


def sjoin_and_proportion(
    sdf_parcels: SparkDataFrame,
    sdf_features: SparkDataFrame,
    columns: list[str],
):
    """Join a parcels data frame to a features dataframe and calculate the
    proportion of each parcel that is overlapped by features.

    Splits multipart geometries into single part as this is required for the groupby
    operation to work.

    Parameters:
        sdf_parcels: The parcels dataframe.
        sdf_features: The features dataframe.
        columns: Columns in the features dataframe to include in the group by when calculating
            the proportion value.
    """
    return (
        sjoin(
            sdf_parcels,
            sdf_features,
        )
        # join in area, required to handle multi polygons
        .join(
            sdf_parcels.groupby("id_parcel").agg(F.expr("SUM(ST_Area(geometry)) as area_parcel")),
            on="id_parcel",
        )
        .groupby("id_parcel", "area_parcel", *columns)
        .agg(F.expr("SUM(ST_Area(ST_Intersection(geometry_left, geometry_right))) as overlap"))
        .select("id_parcel", *columns, F.expr("LEAST(GREATEST(overlap/area_parcel, 0), 1) as proportion"))
    )


def join_parcels(
    parcels: Dataset,
    features: Dataset,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Spatial join the two datasets and calculate the proportion of the parcel that intersects.

    Parameters:
        - parcels: The RPA `reference_parcels` `Dataset`
        - features: The dataset to join in, assumed to be comprised of polygons.
        - columns: The columns in `features` to be included (on top of `geometry`).
        - simplify_tolerence: The tolerance to simplify geometries to (in both datasets).
            Defaults to 20m (assuming SRID 27700).
        - max_vertices: The features polygons will be subdivided and exploded to reduce them
            to this number of vertices to improve performance and memory use. Defaults to 256.

    Returns:
        - A Pandas dataframe with `id_parcel`, `proportion` and columns included in the `columns` list.
    """
    simplify_tolerence = 1.0
    max_vertices = 256
    if columns is None:
        columns = []

    sdf_parcels = parcels.sdf().repartition(1_000)
    sdf_features = (
        features.sdf()
        .repartition(1_000)
        .withColumn("geometry", load_geometry(encoding_fn="", simplify_tolerance=simplify_tolerence))
        .withColumn("geometry", F.expr(f"ST_SubDivideExplode(geometry, {max_vertices})"))
    )

    return sjoin_and_proportion(
        sdf_parcels,
        sdf_features,
        columns=columns,
    ).toPandas()
