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


from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import sjoin_parcel_proportion

from .rpa_reference_parcels import reference_parcels


# RoFRS
class EARoFRSRaw(DataFrameModel):
    """Model for the Risk of Flooding from Rivers and Sea (RoFRS) dataset
    Attributes:
      prob_4band: risk of flooding split into 4 likelihood categories high(>3.3% AEP),
                  medium(between 3.3% and 1% AEP), low(between 1% and 0.1% AEP), very low(<0.1% AEP)
      suitabilit: an indication of what geographic scale the data is suitable for
      Pub_date: date model extent was published
      geometry: Geospatial polygons in EPSG:27700
    """

    prob_4band: str = Field()
    suitabilit: str = Field()
    Pub_date: str = Field()
    geometry: Geometry(crs=SRID) = Field()


ea_rofrs_raw = SourceDataset(
    name="ea_rofrs_raw",
    level0="bronze",
    level1="ea",
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

    id_parcel: str = Field(unique=True)
    proportion: float = Field(ge=0, le=1)


ea_rofrs_raw_parcels = DerivedDataset(
    is_geo=False,
    name="ea_rofrs_raw_parcels",
    level0="silver",
    level1="ea",
    restricted=False,
    func=sjoin_parcel_proportion,
    dependencies=[reference_parcels, ea_rofrs_raw],
    model=EARoFRSParcels,
)

# Flood Zone 3
