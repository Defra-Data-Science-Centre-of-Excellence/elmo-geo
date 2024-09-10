"""Boundaries from RPA Parcels segmentised.

# Segment Parcel Boundaries
Objective is to create a dataset of linear features which are likely to represent a realistic buffer strip.

Segmentation can be done at each node, it can also be done to split 2 points at a maximum length.  However, this can create very short segments
(especially along bends and corners), and such simplification is also utilised to merge shorter segments together.

| Parameter  | Value |   |
| ---------- | ----- |---|
| Tolerance  |   10m | Collects nearby nodes to a single segment.  Higher means less segments.
| Max Length |   50m | Splits long lengths between 2 nodes.  Lower means more segments.
"""
from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry
from pyspark.sql import functions as F

from elmo_geo.etl import SRID, Dataset, DerivedDataset
from elmo_geo.st.segmentise import segmentise_with_tolerance, st_udf
from elmo_geo.utils.types import SparkDataFrame

from .rpa_reference_parcels import reference_parcels


def segmentise_boundary(dataset: Dataset) -> SparkDataFrame:
    return (
        dataset.sdf()
        .withColumn("geometry", F.expr("ST_Boundary(geometry)"))
        .withColumn("geometry", F.expr("EXPLODE(ST_Dump(geometry))"))
        .transform(lambda sdf: st_udf(sdf, segmentise_with_tolerance, "geometry"))
        .withColumn("geometry", F.expr("EXPLODE(ST_Dump(geometry))"))
        .selectExpr(
            "monotonically_increasing_id() AS fid",
            "id_parcel",
            "geometry",
            "ST_Length(geometry) AS m",
        )
    )


class BoundarySegments(DataFrameModel):
    """Model for segmentised boundaries of RPA Reference Parcels

    Attributes:
        id_parcel:
        id_boundary:
        geometry: LineString geometries in EPSG:27700.
        m: length in meters.
    """

    fid: int = Field(coerce=True, unique=True, alias="objectid")
    id_parcel: str = Field()
    geometry: Geometry(crs=SRID) = Field(coerce=True)


boundary_segments = DerivedDataset(
    name="boundary_segments",
    level0="silver",
    level1="elmo_geo",
    model=BoundarySegments,
    restricted=True,
    func=segmentise_boundary,
    dependencies=[reference_parcels],
)
