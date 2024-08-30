"""Functions for transforming datasets.

For use in `elmo.etl.DerivedDataset.func`.
"""
import geopandas as gpd
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

from elmo_geo.st.geometry import load_geometry
from elmo_geo.st.join import sjoin
from elmo_geo.utils.types import SparkDataFrame

from .etl import Dataset


def combine_wide(*datasets: list[Dataset], sources: list[str] | None = None) -> SparkDataFrame:
    """Join multiple derived datasets together using the rpa parcel id to create one big table.

    Parameters:
        *datasets: dependent data updated names, this will make the table easier to understand
        i.e. replacing the duplicated proportion field with the source dataset name.
        sources: dependent dataset names for joining, these are the derived datasets joind to the RPA parcels
    """
    sdf = None
    for dataset, source in zip(datasets, sources):
        _sdf = dataset.sdf()
        if source is None:
            source = dataset.name
        _sdf = _sdf.withColumnRenamed("proportion", f"proportion_{source}")
        sdf = sdf.join(_sdf, on="id_parcel") if sdf else _sdf
    return sdf.toPandas()


def sjoin_and_proportion(
    sdf_parcels: SparkDataFrame,
    sdf_features: SparkDataFrame,
    columns: list[str],
):
    """Join a parcels data frame to a features dataframe and calculate the
    proportion of each parcel that is overlapped by features.

    Parameters:
        sdf_parcels: The parcels dataframe.
        sdf_features: The features dataframe.
        columns: Columns in the features dataframe to include in the group by when calculating
            the proportion value.
    """

    @F.pandas_udf(T.DoubleType(), F.PandasUDFType.GROUPED_AGG)
    def _udf_overlap(geometry_left, geometry_right):
        geometry_left_first = gpd.GeoSeries.from_wkb(geometry_left)[0]  # since grouping by id_parcel, selecting first g_left gives the parcel geom.
        geometry_right_union = gpd.GeoSeries.from_wkb(geometry_right).union_all(method="unary")  # combine intersecting feature geometries into single geom.
        geometry_intersection = geometry_left_first.intersection(geometry_right_union)
        return max(0, min(1, (geometry_intersection.area / geometry_left_first.area)))

    return (
        sjoin(sdf_parcels, sdf_features)
        .withColumn("geometry_left", F.expr("ST_AsBinary(geometry_left)"))
        .withColumn("geometry_right", F.expr("ST_AsBinary(geometry_right)"))
        .groupby(["id_parcel", *columns])
        .agg(
            _udf_overlap("geometry_left", "geometry_right").alias("proportion"),
        )
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
    max_vertices = 256
    if columns is None:
        columns = []

    sdf_parcels = parcels.sdf().repartition(200)
    sdf_features = (
        features.sdf()
        .repartition(200)
        .withColumn("geometry", load_geometry(encoding_fn=""))
        .withColumn("geometry", F.expr(f"ST_SubDivideExplode(geometry, {max_vertices})"))
    )

    return sjoin_and_proportion(
        sdf_parcels,
        sdf_features,
        columns=columns,
    ).toPandas()
