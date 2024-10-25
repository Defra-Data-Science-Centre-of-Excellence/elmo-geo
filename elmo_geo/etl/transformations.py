"""Functions for transforming datasets.

For use in `elmo.etl.DerivedDataset.func`.
"""
import geopandas as gpd
from pyspark.sql import functions as F

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


def _st_union_right(pdf: PandasDataFrame) -> PandasDataFrame:
    "Select first row with the union of geometry_right."
    return pdf.iloc[:1].assign(geometry_right=gpd.GeoSeries.from_wkb(pdf["geometry_right"]).union_all().wkb)


def sjoin_parcels(
    parcels: Dataset | SparkDataFrame,
    feature: Dataset | SparkDataFrame,
    columns: list[str] = [],
    fn_pre: callable = fn_pass,
    fn_post: callable = fn_pass,
    **kwargs,
):
    """Spatially join features dataset to parcels and groups.
    Returns a geospatial dataframe; id_parcel, *columns, geometry_left, geometry_right.

    Parameters:
        parcels: dataset containing rpa reference parcels.
        feature: dataset is assumed to be a source dataset in EPSG:27700 without any geometry tidying.
        columns: from the feature dataset to keep, and group by.
        fn_pre: transforms sdf_feature after it is loaded, and before joined with parcels, used for filtering and renaming.
        fn_post: transforms sdf output after grouping by, used for filtering and renaming columns.
        **kwargs: passed to elmo_geo.st.sjoin, used for distance joins.
    """
    cols = ["id_parcel", *columns]
    sdf_feature = feature if isinstance(feature, SparkDataFrame) else feature.sdf()
    sdf_parcels = parcels if isinstance(parcels, SparkDataFrame) else parcels.sdf()
    return (
        sdf_feature.transform(auto_repartition)
        .transform(fn_pre)
        .transform(lambda sdf: sjoin(sdf_parcels.transform(auto_repartition), sdf, **kwargs))
        .selectExpr(
            *cols,
            "ST_AsBinary(geometry_left) AS geometry_left",
            "ST_AsBinary(geometry_right) AS geometry_right",
        )
        .transform(lambda sdf: sdf.groupby(*cols).applyInPandas(_st_union_right, sdf.schema))
        .selectExpr(
            *cols,
            "ST_GeomFromWKB(geometry_left) AS geometry_left",
            "ST_GeomFromWKB(geometry_right) AS geometry_right",
        )
        .transform(fn_post)
    )


def sjoin_parcel_proportion(
    parcel: Dataset | SparkDataFrame,
    features: Dataset | SparkDataFrame,
    **kwargs,
):
    "Spatially joins datasets, groups, and calculates the proportional overlap, returning a non-geospatial dataframe."
    expr = "ST_CollectionExtract(geometry_right, 3)"
    expr = f"ST_Intersection(geometry_left, {expr})"
    expr = f"ST_Area({expr}) / ST_Area(geometry_left)"
    expr = f"LEAST(GREATEST({expr}, 0), 1)"
    return sjoin_parcels(parcel, features, **kwargs).withColumn("proportion", F.expr(expr)).drop("geometry_left", "geometry_right").toPandas()


def sjoin_boundary_proportion(
    parcel: Dataset | SparkDataFrame,
    boundary_segments: Dataset | SparkDataFrame,
    features: Dataset | SparkDataFrame,
    buffers: list[float] = [0, 2, 8, 12, 24],
    **kwargs,
):
    """Spatially joins with parcels, groups, key joins with boundaries, calculating proportional overlap for multiple buffer distances.
    Returns a non-geospatial dataframe.
    """
    sdf_segments = boundary_segments if isinstance(boundary_segments, SparkDataFrame) else boundary_segments.sdf()

    expr = "ST_Buffer(geometry_right, {})"
    expr = f"ST_Intersection(geometry, {expr})"
    expr = f"ST_Length({expr}) / ST_Length(geometry)"
    expr = f"LEAST(GREATEST({expr}, 0), 1)"
    return (
        sjoin_parcels(parcel, features, distance=max(buffers), **kwargs)
        .join(sdf_segments, on="id_parcel")
        .withColumns({f"proportion_{buf}m": F.expr(expr.format(buf)) for buf in buffers})
        .drop("geometry", "geometry_left", "geometry_right")
    )
