"""Functions for transforming datasets.

For use in `elmo.etl.DerivedDataset.func`.
"""
import pandas as pd
from pyspark.sql import functions as F

from elmo_geo.st.geometry import load_geometry
from elmo_geo.st.join import sjoin

from .etl import Dataset


def _agg_calc_proportion(
    geometry_left: str = "geometry_left",
    geometry_right: str = "geometry_right",
    column: str = "proportion",
    geometry_dim: int = 0,
) -> callable:
    l, r = f"ST_Union_Aggr({geometry_left})", f"ST_Union_Aggr({geometry_right})"
    string = f"ST_Intersection({l}, {r})"
    if geometry_dim:
        string = f"ST_CollectionExtract({string}, {geometry_dim})"
    string = f"ST_Area({string}) / ST_Area({l})"
    string = f"LEAST(GREATEST({string}, 0), 1)"
    return F.expr(f"{string} AS {column}")


def join_parcels(
    parcels: Dataset,
    features: Dataset,
    columns: list[str] | None = None,
    simplify_tolerence: float = 1.0,
    max_vertices: int = 256,
    geometry_dim: int = 0,
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
        - geometry_dim: Only select geometries of this dimension after intersection.
            0 means any, 1 = Points, 2 = LineStrings, 3 = Polygons, Multi is included.

    Returns:
        - A Pandas dataframe with `id_parcel`, `proportion` and columns included in the `columns` list.
    """
    if columns is None:
        columns = []
    df_parcels = (
        parcels.sdf()
        .select("id_parcel", "geometry")
        .withColumn("geometry", load_geometry(encoding_fn="", simplify_tolerence=simplify_tolerence))
    )
    df_feature = (
        features.sdf()
        .select("geometry", *columns)
        .withColumn("geometry", load_geometry(encoding_fn="", simplify_tolerence=simplify_tolerence))
        .withColumn("geometry", F.expr(f"ST_SubdivideExplode(geometry, {max_vertices})"))
    )
    return (
        sjoin(df_parcels, df_feature)
        .groupby("id_parcel", *columns)
        .agg(_agg_calc_proportion())
        .toPandas()
    )
