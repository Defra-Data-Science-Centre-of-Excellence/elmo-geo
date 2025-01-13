"""Parcel level tree features.

Combines boundary segments datasets with boundary and interior tree counts to give numbers for total, interior and boundary trees
in each parcel. Dataset also gives counts of waterbody and hedgerow trees, as well as counts adjusting for adjacency with neighbouring
parcels.
"""

from pandera import DataFrameModel, Field
from pandera.dtypes import Int32
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from elmo_geo.etl import Dataset, DerivedDataset

from .boundary import THRESHOLD_FN, boundary_adjacencies, boundary_hedgerows, boundary_water_2m
from .fcp_tree_detection import (
    fcp_boundary_tree_count,
    fcp_interior_tree_count,
)


# Merger
def _calculate_parcel_tree_counts(
    fcp_boundary_tree_count: Dataset,
    boundary_adjacencies: Dataset,
    boundary_hedgerows: Dataset,
    boundary_water: Dataset,
    fcp_interior_tree_count: Dataset,
    tree_count_buffers: list[int] = [4, 8],
) -> SparkDataFrame:
    """Join boundary datasets with boundary and interior tree counts and aggregate to parcel level.

    Parameters:
        fcp_boundary_tree_count: Boundary tree count dataset.
        boundary_adjacencies: Boundaries shared with other parcels dataset.
        boundary_hedgerows: Boundary hedgerows dataset.
        boundary_water: Boundary waterbody dataset.
        fcp_interior_tree_count: Interior tree count dataset.
        tree_count_buffers: Distances from boundary to base boundary and interior tree counts on. Options are 2,4,8,12,24.
    """

    boundary_types = ["hedgerow", "water"]
    return (
        fcp_boundary_tree_count.sdf()
        .join(
            boundary_adjacencies.sdf()
            .withColumn("bool_adjacency", F.expr(f"CAST({THRESHOLD_FN} AS DOUBLE)"))
            .groupby("id_boundary")
            .agg(
                F.first("m").alias("m"),
                F.max("bool_adjacency").alias("bool_adjacency"),
            ),
            on="id_boundary",
            how="left",
        )
        .join(boundary_hedgerows.sdf().selectExpr("id_boundary", f"CAST({THRESHOLD_FN} AS DOUBLE) AS bool_hedgerow"), on="id_boundary", how="outer")
        .join(boundary_water.sdf().selectExpr("id_boundary", f"CAST({THRESHOLD_FN} AS DOUBLE) AS bool_water"), on="id_boundary", how="outer")
        .withColumn("adj_fraction", F.expr("(2 - bool_adjacency) / 2"))  # Buffer Strips are double sided, adjacency makes this single sided.
        .groupby("id_parcel")
        .agg(
            *[F.expr(f"CAST(SUM(count_{b}m) AS Int) AS n_boundary_trees_{b}m") for b in tree_count_buffers],
            *[F.expr(f"CAST(SUM(count_{b}m * bool_{t}) AS Int) AS n_{t}_trees_{b}m") for b in tree_count_buffers for t in boundary_types],
            *[F.expr(f"SUM(count_{b}m * adj_fraction) AS n_adj_boundary_trees_{b}m") for b in tree_count_buffers],
            *[F.expr(f"SUM(count_{b}m * bool_{t} * adj_fraction) AS n_adj_{t}_trees_{b}m") for b in tree_count_buffers for t in boundary_types],
        )
        .join(
            fcp_interior_tree_count.sdf().selectExpr("id_parcel", *[f"CAST(count_{b}m AS Int) AS n_interior_trees_{b}m" for b in tree_count_buffers]),
            on="id_parcel",
            how="outer",
        )
        .na.fill(0)
        .withColumns(
            {
                "n_adj_trees_4m": F.expr("n_adj_boundary_trees_4m + n_interior_trees_4m"),
                "n_adj_trees_8m": F.expr("n_adj_boundary_trees_8m + n_interior_trees_8m"),
            },
        )
    )


class SylvanFeaturesModel(DataFrameModel):
    """Model for parcel boundary tree features.

    Attributes:
        id_parcel: Parcel id in which that boundary came from.
        n_boundary_trees_*m: Number of trees intersecting parcel boundary at *m buffer distance.
        n_adj_boundary_trees_*m: Number of trees intersecting parcel boudnary at *m buffer distance,
            adjusted for parcel adjacency to avoid double counting.
        n_^_trees_*m: Number of trees intersecting boudnary type ^ at * buffer distance.
        n_adj_^_trees_*m: Number of trees intersecting boudnary type ^ at * buffer distance,
            adjusted for adjacency to avoid double counting.
        n_interior_trees_*m: Number of trees intersecting parcel interior based on *m buffer applied to
            parcel boundary.
        n_trees_*m: Total number of trees in the parcel, adjusted for trees overlapping adjacent parcels.
            This field is given by the sum of n_adj_boundary_trees_*m and n_interior_trees_*m.
    """

    id_parcel: str = Field()
    n_boundary_trees_4m: Int32 = Field()
    n_boundary_trees_8m: Int32 = Field()
    n_adj_boundary_trees_4m: float = Field()
    n_adj_boundary_trees_8m: float = Field()
    n_hedgerow_trees_4m: Int32 = Field()
    n_hedgerow_trees_8m: Int32 = Field()
    n_adj_hedgerow_trees_4m: float = Field()
    n_adj_hedgerow_trees_8m: float = Field()
    n_water_trees_4m: Int32 = Field()
    n_water_trees_8m: Int32 = Field()
    n_adj_water_trees_4m: float = Field()
    n_adj_water_trees_8m: float = Field()
    n_interior_trees_4m: Int32 = Field()
    n_interior_trees_8m: Int32 = Field()
    n_adj_trees_4m: float = Field()
    n_adj_trees_8m: float = Field()


fcp_parcel_sylvan_features = DerivedDataset(
    source="fcp",
    medallion="silver",
    name="fcp_parcel_sylvan_features",
    model=SylvanFeaturesModel,
    restricted=False,
    is_geo=False,
    func=_calculate_parcel_tree_counts,
    dependencies=[fcp_boundary_tree_count, boundary_adjacencies, boundary_hedgerows, boundary_water_2m, fcp_interior_tree_count],
)
"""Parcel tree counts.

Report the total number of trees per parcel as well as number of boundary, hedgerow, waterbody, and interior trees.
Derived from the FCP Tree Detection dataset which was produced using the Environment Agency Vegitation Object Model
raster dataset.
"""
