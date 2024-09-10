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


def combine_wide(
    *datasets: list[Dataset],
    sources: list[str] | None = None,
    keys: list[str] = ["id_parcel"],
    rename_cols: list[str] = ["proportion"],
) -> SparkDataFrame:
    """Join multiple Datasets together to create a wide table.

    Parameters:
        *datasets: Datasets to join together.
        sources: Dataset shorthand names.
        keys: used to group on.
        rename_cols: to be renamed with "{col}_{source}".
    """
    sdf = None
    sources = sources or [None] * len(datasets)
    for dataset, source in zip(datasets, sources):
        source = source or dataset.name
        _sdf = dataset.sdf().withColumnsRenamed({col: f"{col}_{source}" for col in rename_cols})
        sdf = sdf.join(_sdf, on=keys) if sdf else _sdf
    return sdf.toPandas()


def combine_long(
    *datasets: list[Dataset],
    sources: list[str] | None = None,
) -> SparkDataFrame:
    """Append multiple Datasets together to create a long table.

    Parameters:
        *datasets: Datasets to join together. Must contain an 'id_parcel' field.
        sources: Dataset shorthand names. Used to rename 'proportion' fields.
    """
    sdf = None
    sources = sources or [None] * len(datasets)
    for dataset, source in zip(datasets, sources):
        source = source or dataset.name
        _sdf = dataset.sdf().withColumn("source", F.lit(source))
        sdf = sdf.unionByName(_sdf, allowMissingColumns=True) if sdf else _sdf
    return sdf


def fn_pass(sdf: SparkDataFrame) -> SparkDataFrame:
    return sdf


def sjoin_and_proportion(
    sdf_parcels: SparkDataFrame,
    sdf_features: SparkDataFrame,
    columns: list[str] | None = None,
    fn_pre: callable = fn_pass,
    fn_post: callable = fn_pass,
    **sjoin_kwargs,
) -> SparkDataFrame:
    """Join a parcels data frame to a features dataframe and calculate the
    proportion of each parcel that is overlapped by features.

    Parameters:
        sdf_parcels: The parcels dataframe.
        sdf_features: The features dataframe.
        columns: Columns in the features dataframe to include in the group by when calculating
            the proportion value.
    """
    if columns is None:
        columns = []

    @F.pandas_udf(T.DoubleType(), F.PandasUDFType.GROUPED_AGG)
    def _udf_overlap(geometry_left, geometry_right) -> float:
        geometry_left_first = gpd.GeoSeries.from_wkb(geometry_left)[0]  # since grouping by id_parcel, selecting first g_left gives the parcel geom.
        geometry_right_union = gpd.GeoSeries.from_wkb(geometry_right).union_all(method="unary")  # combine intersecting feature geometries into single geom.
        geometry_intersection = geometry_left_first.intersection(geometry_right_union)
        return max(0, min(1, (geometry_intersection.area / geometry_left_first.area)))

    return (
        sdf_features
        .transform(fn_pre)
        .transform(lambda sdf: sjoin(sdf_parcels, sdf, **sjoin_kwargs))
        .withColumn("geometry_left", F.expr("ST_AsBinary(geometry_left)"))
        .withColumn("geometry_right", F.expr("ST_AsBinary(geometry_right)"))
        .groupby(["id_parcel", *columns])
        .agg(
            _udf_overlap("geometry_left", "geometry_right").alias("proportion"),
        )
        .transform(fn_post)
    )


def join_parcels(parcels: Dataset, features: Dataset, **kwargs) -> pd.DataFrame:
    """Spatial join the two datasets and calculate the proportion of the parcel that intersects.

    Parameters:
        - parcels: The RPA `reference_parcels` `Dataset`
        - features: The dataset to join in, assumed to be comprised of polygons.
        - kwargs: keyword arguments for `sjoin_and_proportion`.

    Returns:
        - A Pandas dataframe with `id_parcel`, `proportion` and columns included in the `columns` list.
    """
    return (
        features.sdf()
        .withColumn("geometry", load_geometry(encoding_fn=""))
        .withColumn("geometry", F.expr("ST_SubDivideExplode(geometry, 256)"))
        .transform(lambda sdf: sjoin_and_proportion(parcels.sdf(), sdf, **kwargs))
        .toPandas()
    )
