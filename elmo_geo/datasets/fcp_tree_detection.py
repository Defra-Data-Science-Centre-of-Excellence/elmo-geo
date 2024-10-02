"""The 'tree_features' dataset is parcel level counts of the number of trees
within a parcel and intersecting the perimeter of a parcel.
It was created by DSMT FCP in the ots-sylvan-tree-features' branch of elmo-geo,
in the notebooks/sylvan/Tree Features notebook, which makes use of functions in the
notebooks/sylvan/tree_features.py file.
"""


from pandera import DataFrameModel, Field

from elmo_geo.etl import SourceDataset


#tree detections
class TreeDetectionsRaw(DataFrameModel):
    """Model for fcp tree detection datset.

    Attributes:
        SHEET_ID: Is the parcel sheet ID.
        PARCEL_ID: Is the parcel parcel ID.
        Perimeter_length: The length of the parcel perimeter. Calculated by getting the length of the boundary of the parcel polygon.
        SHEET_PARCEL_ID: Unique identifier for each parcel. Not duplicated, Produced by concatenating REF_PARCEL_SHEET_ID and REF_PARCEL_PARCEL_ID.
        hrtrees_count2: Number of hedgerow trees associated with this parcel.Given by intersecting 2m buffered hedgerow geometries with tree crown coordinates.
        wbtrees_count2: Number of water body trees associated with this parcel.
                        Water bodies are filtered to exclude geometries tagged as ‘Sea’. The amount water bodies are buffered by 2m.
        wbtrees_count4: Number of water body trees associated with this parcel.
                        Water bodies are filtered to exclude geometries tagged as ‘Sea’. The amount water bodies are buffered by 4m.
        perim_trees_count2: Number of trees that intersect with the parcel’s perimeter. The distance the parcel perimeter is buffered by 2m.
        crown_perim_length2: The length of the parcel perimeter that intersects with perimeter tree crowns.
                            Calculated by intersecting the crown geometries (polygons) of perimeter trees
                            (trees whose crown coordinate intersects with the 2m buffered perimeter) with the parcel perimeter.
        int_trees_count2: Number of trees in the parcel interior.
                        The parcel interior is given by the difference between the 2m buffered parcel perimeter and the parcel geometry.
        perim_trees_count4: Number of trees that intersect with the parcel’s perimeter. The distance the parcel perimeter is buffered by 4m.
        crown_perim_length4: The length of the parcel perimeter that intersects with perimeter tree crowns.
                            Calculated by intersecting the crown geometries (polygons) of perimeter trees
        (trees whose crown coordinate intersects with the 4m buffered perimeter) with the parcel perimeter.
        int_trees_count4: Number of trees in the parcel interior.
                        The parcel interior is given by the difference between the 4m buffered parcel perimeter and the parcel geometry.
    """

    SHEET_ID: str = Field()
    PARCEL_ID: str = Field()
    Perimeter_length: float = Field()
    SHEET_PARCEL_ID: str = Field(unique=True)
    hrtrees_count2: int = Field()
    wbtrees_count2: int = Field()
    wbtrees_count4: int = Field()
    perim_trees_count2: int = Field()
    crown_perim_length2: float = Field()
    int_trees_count2: int = Field()
    perim_trees_count4: int = Field()
    crown_perim_length4: float = Field()
    int_trees_count4: int = Field()


    fcp_tree_detection_raw = SourceDataset(
    name="fcp_tree_detection_raw",
    level0="bronze",
    level1="fcp",
    model=TreeDetectionsRaw,
    restricted=False,
    source_path="dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/tree_detections/tree_detections_202311231323.parquet",
)
