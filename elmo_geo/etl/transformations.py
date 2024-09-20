"""Functions for transforming datasets.

For use in `elmo.etl.DerivedDataset.func`.
"""
import geopandas as gpd
from pyspark.sql import functions as F

from elmo_geo.io.file import auto_repartition
from elmo_geo.st.geometry import load_geometry
from elmo_geo.st.join import sjoin
from elmo_geo.utils.types import PandasDataFrame, SparkDataFrame

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


def _st_union_right(pdf: PandasDataFrame) -> PandasDataFrame:
    "Select first row with the union of geometry_right."
    return pdf.iloc[:1].assign(geometry_right=gpd.GeoSeries.from_wkb(pdf["geometry_right"]).union_all().wkb)


def sjoin_parcels(
    parcels: Dataset,
    feature: Dataset,
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
    return (
        feature.sdf()
        .transform(auto_repartition)  # TODO: move to SourceDataset, DerivedDatasets should be well partitioned/
        .withColumn("geometry", load_geometry(encoding_fn="", subdivide=True))  # TODO: ^
        .transform(fn_pre)
        .transform(lambda sdf: sjoin(parcels.sdf(), sdf, **kwargs))
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
    parcel: Dataset,
    features: Dataset,
    **kwargs,
):
    "Spatially joins datasets, groups, and calculates the proportional overlap, returning a non-geospatial dataframe."
    expr = "ST_Intersection(geometry_left, geometry_right)"
    expr = f"ST_Area({expr}) / ST_Area(geometry_left)"
    expr = f"GREATEST(LEAST({expr}, 0), 1)"
    return sjoin_parcels(parcel, features, **kwargs).withColumn("proportion", F.expr(expr)).drop("geometry_left", "geometry_right")


def sjoin_boundary_proportion(
    boundary_segments: Dataset,
    parcel: Dataset,
    features: Dataset,
    buffers: list[float] = [0, 2, 8, 12, 24],
    **kwargs,
):
    """Spatially joins with parcels, groups, key joins with boundaries, calculating proportional overlap for multiple buffer distances.
    Returns a non-geospatial dataframe.
    """
    expr = "ST_Intersection(geometry, ST_Buffer(geometry_right, {}))"
    expr = f"ST_Length({expr}) / ST_Length(geometry)"
    expr = f"GREATEST(LEAST({expr}, 0), 1)"
    return (
        sjoin_parcels(parcel, features, distance=max(buffers), **kwargs)
        .join(boundary_segments.sdf(), on="id_parcel")
        .withColumns({f"proportion_{buf}m": F.expr(expr.format(buf)) for buf in buffers})
        .drop("geometry", "geometry_left", "geometry_right")
    )
