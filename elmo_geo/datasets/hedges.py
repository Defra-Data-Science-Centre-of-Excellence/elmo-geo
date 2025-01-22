"""Ecological Focus Areas Control Layer (Hedges) from RPA (RPA Hedges), provided by DASH.

[^DASH: RPA Hedge]: https://app.powerbi.com/groups/de0d7293-1d23-4194-869d-a4ff2ed2d169/reports/c8802134-4f3b-484e-bf14-1ed9f8881450?ctid=770a2450-0227-4c62-90c7-4e38537f1102&pbi_source=linkShare&bookmarkGuid=5370819e-1c3e-4dc4-8abb-089b5eea5108
"""

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, SourceDataset


class RPAHedgesRaw(DataFrameModel):
    """Model for RPA Hedges dataset.

    Attributes:
        sheet_id, parcel_ref: Concatenate to make id_parcel
        adj_*: If not null identified adjacency, if a hedge is shared with another parcel, or wholly within one.
        geometry: geometries in EPSG:27700.
    """

    sheet_id: str = Field(alias="REF_PARCEL_SHEET_ID")
    parcel_ref: str = Field(alias="REF_PARCEL_PARCEL_ID")
    adj_sheet_id: str = Field(nullable=True, alias="ADJACENT_PARCEL_SHEET_ID")
    adj_parcel_ref: str = Field(nullable=True, alias="ADJACENT_PARCEL_PARCEL_ID")
    geometry: Geometry(crs=SRID) = Field()


rpa_hedges_raw = SourceDataset(
    name="rpa_hedges_raw",
    medallion="bronze",
    source="rpa",
    model=RPAHedgesRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_efa_control_layer/format_GEOPARQUET_efa_control_layer/LATEST_efa_control_layer/",
)
