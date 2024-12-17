"""Parcel level tree count features.

Combines boundary segments datasets with boundary and interior tree counts to give
numbers for total, interior and boundary trees in each parcel. Also gives counts of
waterbody and hedgerow trees, as well as counts adjusting for adjacency with neighbouring
parcels.
"""

from pandera import DataFrameModel, Field
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from elmo_geo.etl import Dataset, DerivedDataset

from .boundary import boundary_adjacencies, boundary_hedgerows, boundary_water_2m
from .fcp_tree_detection import (
    fcp_boundary_tree_count,
)


# Merger
def _combine_boundary_sylvan_features(
    fcp_boundary_tree_count: Dataset,
    boundary_adjacencies: Dataset,
    boundary_hedgerows: Dataset,
    boundary_water: Dataset,
    threshold_str_fn: str = "0.5 < proportion_12m",
) -> SparkDataFrame:
    """Join boundary datasets with boundary tree counts and aggregate to parcel level.

    Set a threshold distance to based boundary proportion on. Default is 12m meaning that a feature must
    be within 12m of a aprcel boundary to be considered as intersecting that boundary.
    """
    count_buffers = [4, 12]  # options are 2,4,8,12,24
    boundary_types = ["hedgerow", "water"]
    return (
        fcp_boundary_tree_count.sdf()
        .join(
            boundary_adjacencies.sdf()
            .withColumn("bool_adjacency", F.expr(f"CAST({threshold_str_fn} AS DOUBLE)"))
            .groupby("id_parcel", "id_boundary")
            .agg(
                F.first("m").alias("m"),
                F.max("bool_adjacency").alias("bool_adjacency"),
            ),
            on="id_boundary",
            how="left",
        )
        .join(boundary_hedgerows.sdf().selectExpr("id_boundary", f"CAST({threshold_str_fn} AS DOUBLE) AS bool_hedgerow"), on="id_boundary", how="outer")
        .join(boundary_water.sdf().selectExpr("id_boundary", f"CAST({threshold_str_fn} AS DOUBLE) AS bool_water"), on="id_boundary", how="outer")
        .withColumn("adj_fraction", F.expr("(2 - bool_adjacency) / 2"))  # Buffer Strips are double sided, adjacency makes this single sided.
        .groupby("id_parcel")
        .agg(
            *[F.expr(f"SUM(n_{b}m) AS n_boundary_trees_{b}m") for b in count_buffers],
            *[F.expr(f"SUM(n_{b}m * bool_{t}) AS n_{t}_trees_{b}m") for b in count_buffers for t in boundary_types],
            *[F.expr(f"SUM(n_{b}m * adj_fraction) AS n_adj_boundary_trees_{b}m") for b in count_buffers],
            *[F.expr(f"SUM(n_{b}m * bool_{t} * adj_fraction) AS n_adj_{t}_trees_{b}m") for b in count_buffers for t in boundary_types],
        )
        .na.fill(0)
    )


class BoundaryMerger(DataFrameModel):
    """Model for parcel boundary tree features.

    Attributes:
        id_parcel: Parcel id in which that boundary came from.
        n_boundary_trees_*m: Number of trees intersecting parcel boudnary at *m buffer distance.
        n_adj_boundary_trees_*m: Number of trees intersecting parcel boudnary at *m buffer distance,
            adjusted for parcel adjacency to avoid double counting.
        n_*_trees_*m: Number of trees intersecting boudnary type ^ at * buffer distance.
        n_adj_*_trees_*m: Number of trees intersecting boudnary type ^ at * buffer distance,
            adjusted for adjacency to avoid double counting.
    """

    id_parcel: str = Field()
    n_boundary_trees_4m: int = Field()
    n_boundary_trees_12m: int = Field()
    n_adj_boundary_trees_4m: float = Field()
    n_adj_boundary_trees_12m: float = Field()
    n_hedgerow_trees_4m: int = Field()
    n_hedgerow_trees_12m: int = Field()
    n_adj_hedgerow_trees_4m: float = Field()
    n_adj_hedgerow_trees_12m: float = Field()
    n_water_trees_4m: int = Field()
    n_water_trees_12m: int = Field()
    n_adj_water_trees_4m: float = Field()
    n_adj_water_trees_12m: float = Field()


fcp_sylvan_boundary_features = DerivedDataset(
    medallion="silver",
    source="fcp",
    name="fcp_sylvan_boundary_features",
    model=BoundaryMerger,
    restricted=False,
    func=_combine_boundary_sylvan_features,
    dependencies=[fcp_boundary_tree_count, boundary_adjacencies, boundary_hedgerows, boundary_water_2m],
    is_geo=False,
)
