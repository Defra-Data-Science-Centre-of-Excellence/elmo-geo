"""
A collection of flood risk mapping products produced by the Environment Agency.

There are two datasets described below:

1 - Flood Map for Planning, Flood Zone 3
  Flood Zone 3 is a best estimate of the areas of land at risk of flooding, when the presence of flood defences are ignored
  and covers land with a 1 in 100 (1%) or greater chance of flooding each year from Rivers; or with a 1 in 200 (0.5%)
  or greater chance of flooding each year from the Sea.


2 - Risk of Flooding from Rivers amnd Sea(RoFRS)
  The dataset shows the chance of flooding from rivers and/or the sea, based on cells of 50m.
  Each cell is allocated one of four flood risk categories,
  taking into account flood defences and their condition.

"""


from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import sjoin_parcel_proportion

from .rpa_reference_parcels import reference_parcels


# RoFRS
class EARoFRSRaw(DataFrameModel):
    """Model for the Risk of Flooding from Rivers and Sea (RoFRS) dataset
    Attributes:
      prob_4band: risk of flooding split into 4 likelihood categories high(>3.3% AEP (annual exceedance probability)),
                  medium(between 3.3% and 1% AEP), low(between 1% and 0.1% AEP), very low(<0.1% AEP)
      suitabilit: an indication of what geographic scale the data is suitable for
      pub_date: date model extent was published
      geometry: Geospatial polygons in EPSG:27700
    """

    prob_4band: str = Field()
    suitabilit: str = Field()
    pub_date: str = Field()
    geometry: Geometry(crs=SRID) = Field()


ea_rofrs_raw = SourceDataset(
    name="ea_rofrs_raw",
    medallion="bronze",
    source="ea",
    model=EARoFRSRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_risk_of_flooding_from_rivers_and_sea/format_GEOPARQUET_risk_of_flooding_from_rivers_and_sea/LATEST_risk_of_flooding_from_rivers_and_sea/Risk_of_Flooding_from_Rivers_and_Sea.parquet/",
)


class EARoFRSParcels(DataFrameModel):
    """Model for the Risk of Flooding from Rivers and Sea (RoFRS) dataset joined with Rural Payment Agency parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        proportion: The proportion of the parcel that intersects with the RoFRS
    """

    id_parcel: str = Field()
    prob_4band: str = Field(isin=["Low", "Medium", "High"])
    proportion: float = Field(ge=0, le=1)


ea_rofrs_parcels = DerivedDataset(
    is_geo=False,
    name="ea_rofrs_parcels",
    medallion="silver",
    source="ea",
    restricted=False,
    func=partial(sjoin_parcel_proportion, columns=["prob_4band"]),
    dependencies=[reference_parcels, ea_rofrs_raw],
    model=EARoFRSParcels,
)
"""Indicates the proportion of each parcel intersected by the Risk of Flooding from Rivers and Sea dataset. Risk may be low or high
"""


# Flood Zone 3
class EAFZ3Raw(DataFrameModel):
    """Model for Environment Agency Flood Zone 3 (FZ3) taken from the Floodmap for Planning dataset
    Attributes:
    layer: name of layer i.e flood zone 3
    type: type of flooding i.e fluvial or tidal
    geometry: Geospatial polygons in EPSG:27700
    """

    layer: str = Field()
    type: str = Field()
    geometry: Geometry(crs=SRID) = Field()


ea_fz3_raw = SourceDataset(
    name="ea_fz3_raw",
    medallion="bronze",
    source="ea",
    model=EAFZ3Raw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_ea_flood_map_flood_zone_3/format_GEOPARQUET_ea_flood_map_flood_zone_3/LATEST_ea_flood_map_flood_zone_3/ea_flood_map_for_planning_rivers_and_sea_flood_zone_3_n.parquet/",
)


class EAFZ3Parcels(DataFrameModel):
    """Model for Environment Agency Flood Zone 3 (FZ3) taken from the Floodmap for Planning dataset joined with Rural Payment Agency parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        proportion: The proportion of the parcel that intersects with the FZ3.
    """

    id_parcel: str = Field(unique=True)
    proportion: float = Field(ge=0, le=1)


ea_fz3_parcels = DerivedDataset(
    is_geo=False,
    name="ea_fz3_parcels",
    medallion="silver",
    source="ea",
    restricted=False,
    func=sjoin_parcel_proportion,
    dependencies=[reference_parcels, ea_fz3_raw],
    model=EAFZ3Parcels,
)
"""Indicates the proportion of each parcel intersected by the flood zone 3 dataset.
"""
