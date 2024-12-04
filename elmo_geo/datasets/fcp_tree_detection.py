from pandera import DataFrameModel, Field
from pandera.dtypes import Float64

from elmo_geo.etl import SourceDataset


# Tree Detctions Dataset - potentially temporay data used for a specific BTO request
class FCPTreeDetectionsRaw(DataFrameModel):
    """Model for raw tree detection data before parcel joins and counts.
    Attributes:
        top_x:easting spatial reference for point location of a tree
        top_y:northing spatial reference for point location of a tree
        top_height: height of identified tree (maybe meters, need to confirm)
        chm_path:source of lidar the tree detection is derived from
        msg: field for annotations
        top_point: point geometry
        crown_poly_raster: polygon geometry of the crown of the tree
        major_grid:possibly OS grid location i.e. SO

    """

    top_x: Float64 = Field()
    top_y: Float64 = Field()
    top_height: Float64 = Field()
    chm_path: str = Field()
    msg: str = Field()
    top_point: str = Field()
    crown_poly_raster: str = Field()
    major_grid: str = Field()


fcp_tree_detection_raw = SourceDataset(
    name="fcp_tree_detection_raw",
    level0="bronze",
    level1="fcp",
    model=FCPTreeDetectionsRaw,
    restricted=False,
    is_geo=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm/elmo/tree_features/tree_detections/tree_detections_202311231323.parquet/",
)
"""The 'tree_features' dataset is parcel level counts of the number of trees
within a parcel and intersecting the perimeter of a parcel.
It was created by DSMT FCP in the ots-sylvan-tree-features' branch of elmo-geo,
in the notebooks/sylvan/Tree Features notebook, which makes use of functions in the
notebooks/sylvan/tree_features.py file
"""
