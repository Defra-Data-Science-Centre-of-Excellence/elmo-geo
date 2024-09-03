"""Cranfield Environment Centre (CEC) SoilScapes data.

The CEC SoilSacpaes dataset is a product derived from the National Soil Map. It seeks to provide a useful, concise, easily interpreted and
applicable description of the soils of England and Wales. It is a simplified rendition of the national soil map and contains about
30 distinct soil types.
"""


from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SourceDataset


class CECSoilScapesRaw(DataFrameModel):
    """Cranfield Environment Centre (CEC) SoilScapes data model.

    Parameters:
        geometry: The sensitivity classification's geospatial extent (polygons).
    """

    geometry: Geometry = Field(coerce=True, nullable=False)


ceh_soilscapes_raw = SourceDataset(
    name="ceh_soilscapes_raw",
    level0="bronze",
    level1="cec",
    restricted=True,
    is_geo=True,
    source_path="/dbfs/mnt/lab/restricted/natmap/natmap_soilscapes.parquet",
)
"""Cranfield Environment Centre (CEC) SoilScapes data.
"""
