"""Datasets produced by the Farming and Countryside Programme - Evidence and Analysis team relating to sylvan (aka woody) features.

The relict hedges dataset used is was produces by the notebook '/notebooks/sylvan/Relict Hedges' and was exported with the notebook
'/notebooks/sylvan/Download Relict Hedges'. These notebooks no longer run successfully and there is an outstanding task to update them
(https://github.com/Defra-Data-Science-Centre-of-Excellence/elmo-geo/issues/238).


"""


from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, SourceDataset


class RelictHedgeRaw(DataFrameModel):
    """Model for relict hedges dataset.

    Attributes:
        geometry: Section of parcel boundary classified as a relict hedge.
    """

    geometry: Geometry(crs=SRID) = Field(nullable=True)


fcp_relict_hedge_raw = SourceDataset(
    name="fcp_relict_hedge_raw",
    medallion="silver",
    source="fcp",
    model=RelictHedgeRaw,
    restricted=False,
    source_path="/dbfs/mnt/lab/unrestricted/ELM-Project/bronze/fcp/relict_parcel_boundaries.geojson",
)
"""
This dataset comprises geometries that are sections of June 2021 parcel boundaries classified as relict hedge.
These are defined as non-woodland or hedgerow sections of parcel boundaries that have above a threshold density of trees.

The threshold is set to 30% of parcel boundary segment length intersected by tree crowns. This is a permissive threshold and
may in places classify section of boundaries with large trees in as a relict hedge.

Further details on the methodology as below.

1. Intersect parcel boundaries with NFI woodland and EFA hedges data. Remove these sections of th boundary.
2. Intersect the remaining boundary with Defra Tree Map (trees detected from EA Vegetation Object Model dataset)
3. Classify sections of remaining parcel boundary as relict hedge based on the density of tree canopy it intersects with.

The classification rules:
1. Split parcel boundary into straght line segments
2. Exclude segments <2m from relict classification
3. Classify remaining sements if >30% of segment is intersected by tree canopy
"""
