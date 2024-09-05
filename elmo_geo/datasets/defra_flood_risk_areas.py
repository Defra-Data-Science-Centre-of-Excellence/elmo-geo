"""Flood Risk Areas from Defra, provided by DASH.

[^DASH: Flood Risk Areas]: https://app.powerbi.com/Redirect?action=OpenReport&appId=5762de14-3aa8-4a83-92b3-045cc953e30c&reportObjectId=c8802134-4f3b-484e-bf14-1ed9f8881450&ctid=770a2450-0227-4c62-90c7-4e38537f1102&reportPage=ReportSectionf8b0041ad0335117bacb&pbi_source=appShareLink&portalSessionId=dcf83a8a-cd44-4b33-8d11-06f005e1dbac
"""
from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels


class FloodRiskAreasRaw(DataFrameModel):
    """Model for Defra Flood Risk Areas dataset.

    Attributes:
        fra_id: Feature ID e.g. "UK04A0001ENG".
        fra_name: Name of fra_id, e.g. "Kingston upon Hull and Haltemprice, Humber".
        frr_cycle: 1, 2.
        flood_sour: "Surface Water", "Rivers and Sea".
        geometry: Flood Risk Area geometries in EPSG:27700.
    """

    fra_id: str = Field(coerce=True, unique=False)
    fra_name: str = Field(coerce=True)
    frr_cycle: int = Field(coerce=True)
    flood_sour: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


flood_risk_areas_raw = SourceDataset(
    name="flood_risk_areas_raw",
    level0="bronze",
    level1="defra",
    model=FloodRiskAreasRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_flood_risk_areas/format_GEOPARQUET_flood_risk_areas/LATEST_flood_risk_areas/Flood_Risk_Areas.parquet",
)


class FloodRiskAreasParcels(DataFrameModel):
    """Model for Defra Flood Risk Areas with parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        geometry: Flood Risk Area geometries in EPSG:27700.
    """

    id_parcel: str = Field()
    proportion: float = Field(ge=0, le=1)


flood_risk_areas_parcels = DerivedDataset(
    is_geo=False,
    name="flood_risk_areas_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    func=join_parcels,
    dependencies=[reference_parcels, flood_risk_areas_raw],
    model=FloodRiskAreasParcels,
)
