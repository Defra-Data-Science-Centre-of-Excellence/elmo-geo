"""Functions for transforming datasets.

For use in `elmo.etl.DerivedDataset.func`.
"""
import geopandas as gpd
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

from elmo_geo.st.geometry import load_geometry
from elmo_geo.st.join import sjoin
from elmo_geo.utils.types import Geometry, SparkDataFrame

from .etl import Dataset


def combine(
    *datasets: list[Dataset],
    sources: list[str] | None = None,
) -> SparkDataFrame:
    sdf = None
    for dataset, source in zip(datasets, sources):
        _sdf = dataset.sdf()
        if source is None:
            source = dataset.name
        _sdf = _sdf.withColumn("source", F.lit(source))
        if sdf is None:
            sdf = _sdf
        else:
            sdf.unionByName(_sdf, allowMissingColumns=True)
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
    def _udf_overlap(geometry_left: Geometry, geometry_right: Geometry) -> float:
        geometry_left_first = gpd.GeoSeries.from_wkb(geometry_left)[0]  # since grouping by id_parcel, selecting first g_left gives the parcel geom.
        geometry_right_union = gpd.GeoSeries.from_wkb(geometry_right).union_all(method="unary")  # combine intersecting feature geometries into single geom.
        geometry_intersection = geometry_left_first.intersection(geometry_right_union)
        return max(0, min(1, (geometry_intersection.area / geometry_left_first.area)))

    return (
        sdf_features.transform(fn_pre)
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
