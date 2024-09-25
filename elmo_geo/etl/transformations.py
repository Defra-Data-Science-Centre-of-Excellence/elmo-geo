"""Functions for transforming datasets.

For use in `elmo.etl.DerivedDataset.func`.
"""
from functools import partial

import geopandas as gpd
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from elmo_geo.io.file import auto_repartition
from elmo_geo.st.geometry import load_geometry
from elmo_geo.st.join import sjoin
from elmo_geo.utils.types import PandasDataFrame, SparkDataFrame

from .etl import Dataset


def pivot_long_sdf(
    sdf: SparkDataFrame,
    columns: list[str],
    name_col: str = "name",
    value_col: str = "value",
) -> SparkDataFrame:
    other_cols = set(sdf.columns).difference(columns)
    return sdf.selectExpr(
        *other_cols,
        "STACK({}, {}) AS ({}, {})".format(
            len(columns),
            ", ".join(f"'{col}', {col}" for col in columns),
            name_col,
            value_col,
        ),
    )


def pivot_wide_sdf(
    sdf: SparkDataFrame,
    name_col: str = "name",
    value_col: str = "value",
) -> SparkDataFrame:
    other_cols = set(sdf.columns).difference([name_col, value_col])
    return sdf.groupby(*other_cols).pivot(name_col).agg(F.first(value_col))


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


def _st_parcel_proportion(pdf: PandasDataFrame) -> PandasDataFrame:
    "Calculate the proportion overlap between a parcel geometry and its overlapping features"
    geometry_left_first = gpd.GeoSeries.from_wkb(pdf["geometry_left"])[0]  # since grouping by id_parcel, selecting first g_left gives the parcel geom.
    geometry_right_union = gpd.GeoSeries.from_wkb(pdf["geometry_right"]).union_all()  # combine intersecting feature geometries into single geom.
    geometry_intersection = geometry_left_first.intersection(geometry_right_union)
    return (
        pdf.iloc[:1]
        .assign(proportion=max(0, min(1, (geometry_intersection.area / geometry_left_first.area))))
        .drop(["geometry_left", "geometry_right"], axis=1)
    )


def _st_boundary_proportions(pdf: PandasDataFrame, buffers: list[float] = []) -> PandasDataFrame:
    "Calculate the proportion overlap between parcel boundary segments its overlapping features"
    geometry_right_union = gpd.GeoSeries.from_wkb(pdf["geometry_right"]).union_all()  # combine intersecting feature geometries into single geom.
    segments = gpd.GeoSeries.from_wkb(pdf["geometry"])
    for buf in buffers:
        intersection_lengths = segments.map(lambda g: g.intersection(geometry_right_union.buffer(buf)).length)
        pdf[f"proportion_{buf}m"] = (intersection_lengths / segments.length).clip(lower=0, upper=1)
    return pdf.drop(["geometry", "geometry_right", "geometry_left"], axis=1)


def sjoin_parcel_proportion(
    parcels: Dataset | SparkDataFrame,
    features: Dataset | SparkDataFrame,
    columns: list[str] = [],
    fn_pre: callable = fn_pass,
    fn_post: callable = fn_pass,
    **kwargs,
):
    "Spatially joins datasets, groups, and calculates the proportional overlap, returning a non-geospatial dataframe."
    sdf_features = features if isinstance(features, SparkDataFrame) else features.sdf()
    sdf_parcels = parcels if isinstance(parcels, SparkDataFrame) else parcels.sdf()

    cols = ["id_parcel", *columns]
    schema = StructType(
        [StructField("id_parcel", StringType(), False)]
        + [field for field in sdf_features.schema.fields if field.name in columns]
        + [StructField("proportion", FloatType(), False)]
    )

    return (
        sdf_features.transform(auto_repartition)  # TODO: move to SourceDataset, DerivedDatasets should be well partitioned/
        .withColumn("geometry", load_geometry(encoding_fn="", subdivide=True))  # TODO: ^
        .transform(fn_pre)
        .transform(lambda sdf: sjoin(sdf_parcels, sdf, **kwargs))
        .selectExpr(
            *cols,
            "ST_AsBinary(geometry_left) AS geometry_left",
            "ST_AsBinary(geometry_right) AS geometry_right",
        )
        .transform(lambda sdf: sdf.groupby(*cols).applyInPandas(_st_parcel_proportion, schema))
        .transform(fn_post)
        .toPandas()
    )


def sjoin_boundary_proportion(
    boundary_segments: Dataset,
    parcels: Dataset | SparkDataFrame,
    features: Dataset | SparkDataFrame,
    columns: list[str] = [],
    buffers: list[float] = [0, 2, 8, 12, 24],
    fn_pre: callable = fn_pass,
    fn_post: callable = fn_pass,
    **kwargs,
):
    """Spatially joins with parcels, groups, key joins with boundaries, calculating proportional overlap for multiple buffer distances.
    Returns a non-geospatial dataframe.
    """

    sdf_features = features if isinstance(features, SparkDataFrame) else features.sdf()
    sdf_parcels = parcels if isinstance(parcels, SparkDataFrame) else parcels.sdf()
    sdf_boundary_segments = boundary_segments if isinstance(boundary_segments, SparkDataFrame) else boundary_segments.sdf()

    cols = ["id_parcel", *columns]
    schema = StructType(
        [StructField("id_parcel", StringType(), False)]
        + [field for field in sdf_features.schema.fields if field.name in cols]
        + [StructField(f"proportion_{buf}m", FloatType(), False) for buf in buffers]
    )

    return (
        sdf_features.transform(auto_repartition)  # TODO: move to SourceDataset, DerivedDatasets should be well partitioned/
        .withColumn("geometry", load_geometry(encoding_fn="", subdivide=True))  # TODO: ^
        .transform(fn_pre)
        .transform(lambda sdf: sjoin(sdf_parcels, sdf, distance=max(buffers), **kwargs))
        .join(sdf_boundary_segments, on="id_parcel")
        .selectExpr(
            *cols,
            "ST_AsBinary(geometry_left) AS geometry_left",
            "ST_AsBinary(geometry_right) AS geometry_right",
            "ST_AsBinary(geometry) AS geometry",
        )
        .transform(lambda sdf: sdf.groupby(*cols).applyInPandas(partial(_st_boundary_proportions, buffers=buffers), schema))
        .transform(fn_post)
    )
