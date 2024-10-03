"""RPA Land Cover
"""


from pandera import DataFrameModel

from elmo_geo.etl.etl import SourceGlobDataset


class RPALandCoversTableRaw(DataFrameModel):
    """Model for RPA Land Cover data."""


rpa_land_covers_table = SourceGlobDataset(
    name="rpa_land_covers_table",
    level0="bronze",
    level1="rpa",
    model=RPALandCoversTableRaw,
    restricted=False,
    glob_path="/dbfs/mnt/lab/unrestricted/ELM-Project/raw/LandCovers_Table.gdb",
)
