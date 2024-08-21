"""Ecological Focus Areas Control Layer (Hedges) from RPA (RPA Hedges), provided by DASH.

[^DASH: Search "hedge"]:
"""

from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels


class RPAHedgesRaw(DataFrameModel):
    """Model for RPA Hedges dataset.

    Attributes:
        geometry: geometries in EPSG:27700.
    """

    sheet_id: str = Field(coerce=True, alias="REF_PARCEL_SHEET_ID")
    parcel_ref: str = Field(coerce=True, alias="REF_PARCEL_PARCEL_ID")
    adj_sheet_id: str = Field(coerce=True, nullable=True, alias="ADJACENT_PARCEL_SHEET_ID")
    adj_parcel_ref: str = Field(coerce=True, nullable=True, alias="ADJACENT_PARCEL_PARCEL_ID")
    geometry: Geometry(crs=SRID) = Field(coerce=True, alias="GEOM")


rpa_hedges_raw = SourceDataset(
    name="rpa_hedges_raw",
    level0="bronze",
    level1="rpa",
    model=RPAHedgesRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_efa_control_layer/format_GEOPARQUET_efa_control_layer/LATEST_efa_control_layer/",
)


class RPAHedgesParcels(DataFrameModel):
    """Model for RPA Hedges with parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        proportion: proportion of Parcel geometry overlapping with feature geometry.
    """

    id_parcel: str = Field()
    proportion: float = Field(ge=0, le=1)


rpa_hedges_parcels = DerivedDataset(
    name="rpa_hedges_parcels",
    level0="silver",
    level1="rpa",
    restricted=False,
    func=join_parcels,  # TODO: tidy
    dependencies=[reference_parcels, rpa_hedges_raw],
    model=RPAHedgesParcels,
)
