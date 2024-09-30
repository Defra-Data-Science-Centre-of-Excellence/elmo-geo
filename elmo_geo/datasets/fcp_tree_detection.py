"""The 'tree_features' dataset is parcel level counts of the number of trees
within a parcel and intersecting the perimeter of a parcel.
It was created by DSMT FCP in the ots-sylvan-tree-features' branch of elmo-geo,
in the notebooks/sylvan/Tree Features notebook, which makes use of functions in the
notebooks/sylvan/tree_features.py file.
"""

from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import sjoin_parcel_proportion

from .rpa_reference_parcels import reference_parcels


#tree detections
class TreeDetectionsRaw(DataFrameModel):
      """Model for fcp tree detection datset.

    Parameters:
        SHEET_ID: Is the parcel sheet ID.
        PARCEL_ID: Is the parcel parcel ID.
        Perimeter_length: The length of the parcel perimeter. Calculated by getting the length of the boundary of the parcel polygon.
        SHEET_PARCEL_ID: Unique identifier for each parcel. Not duplicated, Produced by concatenating REF_PARCEL_SHEET_ID and REF_PARCEL_PARCEL_ID.
        hrtrees_count2: Number of hedgerow trees associated with this parcel.Given by intersecting 2m buffered hedgerow geometries with tree crown coordinates.
        wbtrees_count2: 
        wbtrees_count4:
        perim_trees_count2:
        crown_perim_length2:
        int_trees_count2:
        perim_trees_count4:
        crown_perim_length4:
        int_trees_count4:

    """

    PARCEL_ID: str = Field()
    Perimeter_length:
    SHEET_PARCEL_ID:
    hrtrees_count2:
    wbtrees_count2: 
    wbtrees_count4:
    perim_trees_count2:
    crown_perim_length2:
    int_trees_count2:
    perim_trees_count4:
    crown_perim_length4:
    int_trees_count4:

    fid: str = Field(unique=True, alias="CTRY23CD")
    name: str = Field(alias="CTRY23NM")
    geometry: Geometry(crs=SRID) = Field()


    fcp_tree_detection_raw = SourceDataset(
    name="fcp_tree_detection_raw",
    level0="bronze",
    level1="fcp",
    model=TreeDetectionsRaw,
    restricted=False,
    source_path="dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/tree_detections/tree_detections_202311231323.parquet",
)
