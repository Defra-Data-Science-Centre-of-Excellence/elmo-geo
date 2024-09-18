"""Ordnance Survey Datasets, provided by DASH.
- National Geographic Database, is the single store of all Ordnance Survey's authoritative data.
- Open GreenSpace
- MasterMap Highways Network Roads Prem

[^os]: https://www.ordnancesurvey.co.uk/products/os-ngd
[^dash]: https://app.powerbi.com/Redirect?action=OpenReport&appId=5762de14-3aa8-4a83-92b3-045cc953e30c&reportObjectId=c8802134-4f3b-484e-bf14-1ed9f8881450&ctid=770a2450-0227-4c62-90c7-4e38537f1102&reportPage=ReportSectionff2a0c223272005d9b10&pbi_source=appShareLink&portalSessionId=f7a19b52-4676-43dd-9f13-d1084081a8f2
"""

from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl.etl import SRID, SourceGlobDataset


class OSNGDRaw(DataFrameModel):
    """Model for merged OS NGD datasets.

    Attributes:
        fid: is the row id.
        osid: Ordnance Survey Identifier is the primary identifier and unique key for this theme.
        toid: Topographic Identifier is an additional secondary identifier which can aid further data linking.
        geometry: Any type geometries in EPSG:27700.
    """

    fid: int = Field(coerce=True, unique=True)
    osid: str = Field(coerce=True, unique=True)
    toid: str = Field(coerce=True)
    theme: str = Field(coerce=True)
    description: str = Field(coerce=True)
    layer: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


os_ngd_raw = SourceGlobDataset(
    name="os_ngd_raw",
    level0="bronze",
    level1="os",
    model=OSNGDRaw,
    restricted=True,
    source_path=None,
    glob_path="/dbfs/mnt/base/restricted/source_ordnance_survey_data_hub/dataset_ngd*/format_GPKG*/LATEST_*/*.gpkg",
)
