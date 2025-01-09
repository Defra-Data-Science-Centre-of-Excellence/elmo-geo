"""Historic and archaeological feature dataset. It combines multiple sources of historic and archaeological
 features from Historic England into a single parquet file, and calculates the proportion of each parcel 
 intersected by each historic or archaeological feature. The source datasets include:

- SHINE: the Selected Heritage Inventory for Natural England
- Listed buildings
- protected_wreck_sites
- registered_battlefields 
- registered_parks_and_gardens
- scheduled_monuments 
- world_heritage_sites
"""


from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, SourceDataset


# Selected Heritage Inventory for Natural England (SHINE)
class ELMSHINERaw(DataFrameModel):
    """Model for the Selected Heritage Inventory for Natural England. It contains non-designated historic and archaeological features from across England.

    Attributes:
       shine_uid: Reference id for the SHINE feature
       shine_name: Name of the SHINE
       geom: Geospatial polygons in EPSG:27700
    """

    shine_uid: str = Field()
    shine_name: str = Field()
    geom: Geometry(crs=SRID) = Field()


elm_shine_raw = SourceDataset(
    name="elm_shine_raw",
    medallion="bronze",
    source="he",
    model=ELMSHINERaw,
    restricted=False,
    source_path="/dbfs/mnt/lab/restricted/ELM-Project/bronze/he-shine-2022_12_30.parquet",
)


# Listed buildings (LB)
class HELBRaw(DataFrameModel):
    """Model for Historic England Listed buildings dataset.

    Attributes:
       list_entry: Reference id for each LB
       name: Name of the LB
       geometry: Geospatial polygons in EPSG:27700
    """

    list_entry: int = Field(alias="ListEntry")
    name: str = Field(alias="Name")
    geometry: Geometry(crs=SRID) = Field()


he_lb_raw = SourceDataset(
    name="he_lb_raw",
    medallion="bronze",
    source="he",
    model=HELBRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_listed_buildings_polys/format_GEOPARQUET_listed_buildings_polys/LATEST_listed_buildings_polys",
)


# Protected Wreck Sites (PWS)
class HEPWSRaw(DataFrameModel):
    """Model for Historic England Sites of Protected Wreck Sites dataset.

    Attributes:
       list_entry: Reference id for each PWS
       name: Name of the PWS
       geometry: Geospatial polygons in EPSG:27700
    """

    list_entry: int = Field(alias="ListEntry")
    name: str = Field(alias="Name")
    geometry: Geometry(crs=SRID) = Field()


he_pws_raw = SourceDataset(
    name="he_pws_raw",
    medallion="bronze",
    source="he",
    model=HEPWSRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_protected_wreck_sites/format_GEOPARQUET_protected_wreck_sites/SNAPSHOT_2024_04_29_protected_wreck_sites/",
)


# Registered Battlefields (RB)
class HERBRaw(DataFrameModel):
    """Model for Historic England Registered Battlefields dataset.

    Attributes:
       list_entry: Reference id for each RB
       name: Name of the RB
       geometry: Geospatial polygons in EPSG:27700
    """

    list_entry: int = Field(alias="ListEntry")
    name: str = Field(alias="Name")
    geometry: Geometry(crs=SRID) = Field()


he_rb_raw = SourceDataset(
    name="he_rb_raw",
    medallion="bronze",
    source="he",
    model=HERBRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_registered_battlefields/format_GEOPARQUET_registered_battlefields/SNAPSHOT_2024_04_29_registered_battlefields/",
)


# Registered Parks and Gardens (RPG)
class HERPGRaw(DataFrameModel):
    """Model for Historic England Registered Parks and Gardens dataset.

    Attributes:
       list_entry: Reference id for each RPG
       name: Name of the RPG
       geometry: Geospatial polygons in EPSG:27700
    """

    list_entry: int = Field(alias="ListEntry")
    name: str = Field(alias="Name")
    geometry: Geometry(crs=SRID) = Field()


he_rpg_raw = SourceDataset(
    name="he_rpg_raw",
    medallion="bronze",
    source="he",
    model=HERPGRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_registered_parks_and_gardens/format_GEOPARQUET_registered_parks_and_gardens/SNAPSHOT_2024_04_29_registered_parks_and_gardens/",
)


# Scheduled Monuments (SM)
class HESMRaw(DataFrameModel):
    """Model for Historic England Scheduled Monuments dataset.

    Attributes:
       list_entry: Reference id for each SM
       name: Name of the SM
       geometry: Geospatial polygons in EPSG:27700
    """

    list_entry: int = Field(alias="ListEntry")
    name: str = Field(alias="Name")
    geometry: Geometry(crs=SRID) = Field()


he_sm_raw = SourceDataset(
    name="he_sm_raw",
    medallion="bronze",
    source="he",
    model=HESMRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_scheduled_monuments/format_GEOPARQUET_scheduled_monuments/SNAPSHOT_2024_04_29_scheduled_monuments/",
)


# World Heritage Sites (WHS)
class HEWHSRaw(DataFrameModel):
    """Model for World Heritage Sites dataset.

    Attributes:
       list_entry: Reference id for each WHS
       name: Name of the WHS
       geometry: Geospatial polygons in EPSG:27700
    """

    list_entry: int = Field(alias="ListEntry")
    name: str = Field(alias="Name")
    geometry: Geometry(crs=SRID) = Field()


he_whs_raw = SourceDataset(
    name="he_whs_raw",
    medallion="bronze",
    source="he",
    model=HEWHSRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_world_heritage_sites/format_GEOPARQUET_world_heritage_sites/SNAPSHOT_2024_04_29_world_heritage_sites/",
)
