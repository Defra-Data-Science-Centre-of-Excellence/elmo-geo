"""Historic and archaeological feature dataset. It combines multiple sources of historic and archaeological features from Historic England into a single parquet file, and calculates the proportion of each parcel intersected by each historic or archaeological feature. The source datasets include:

- SHINE: the Selected Heritage Inventory for Natural England
- protected_wreck_sites
- registered_battlefields 
- registered_parks_and_gardens
- scheduled_monuments 
- world_heritage_sites
"""

from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import combine_wide, sjoin_parcel_proportion

from .rpa_reference_parcels import reference_parcels

# Selected Heritage Inventory for Natural England (SHINE)
class ElmShineRaw(DataFrameModel):
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
    model=ElmShineRaw,
    restricted=False,
    source_path="/dbfs/mnt/lab/restricted/ELM-Project/bronze/he-shine-2022_12_30.parquet",
)


# Protected Wreck Sites (PWS)
class HePwsRaw(DataFrameModel):
    """Model for Historic England Sites of Protected Wreck Sites dataset.

    Attributes: 
       ListEntry: Reference id for each PWS
       Name: Name of the PWS
       geometry: Geospatial polygons in EPSG:27700
    """

    ListEntry: str = Field() 
    Name: str = Field()
    geometry: Geometry(crs=SRID) = Field() 


he_pws_raw = SourceDataset(
    name="he_pws_raw",
    medallion="bronze",
    source="he",
    model=HePwsRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_protected_wreck_sites/format_GEOPARQUET_protected_wreck_sites/SNAPSHOT_2024_04_29_protected_wreck_sites/",
)


# Registered Battlefields (RB)
class HeRbRaw(DataFrameModel):
    """Model for Historic England Registered Battlefields dataset.

    Attributes: 
       ListEntry: Reference id for each RB
       Name: Name of the RB
       geometry: Geospatial polygons in EPSG:27700
    """

    ListEntry: str = Field() 
    Name: str = Field()
    geometry: Geometry(crs=SRID) = Field() 


he_rb_raw = SourceDataset(
    name="he_rb_raw",
    medallion="bronze",
    source="he",
    model=HeRbRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_registered_battlefields/format_GEOPARQUET_registered_battlefields/SNAPSHOT_2024_04_29_registered_battlefields/",
)


# Registered Parks and Gardens (RPG)
class HeRpgRaw(DataFrameModel):
    """Model for Historic England Registered Parks and Gardens dataset.

    Attributes: 
       ListEntry: Reference id for each RPG
       Name: Name of the RPG
       geometry: Geospatial polygons in EPSG:27700
    """

    ListEntry: str = Field() 
    Name: str = Field()
    geometry: Geometry(crs=SRID) = Field() 


he_rpg_raw = SourceDataset(
    name="he_rpg_raw",
    medallion="bronze",
    source="he",
    model=HeRpgRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_registered_parks_and_gardens/format_GEOPARQUET_registered_parks_and_gardens/SNAPSHOT_2024_04_29_registered_parks_and_gardens/",
)


# Scheduled Monuments (SM)
class HeSmRaw(DataFrameModel):
    """Model for Historic England Scheduled Monuments dataset.

    Attributes: 
       ListEntry: Reference id for each SM
       Name: Name of the SM
       geometry: Geospatial polygons in EPSG:27700
    """

    ListEntry: str = Field() 
    Name: str = Field()
    geometry: Geometry(crs=SRID) = Field() 


he_sm_raw = SourceDataset(
    name="he_sm_raw",
    medallion="bronze",
    source="he",
    model=HeSmRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_scheduled_monuments/format_GEOPARQUET_scheduled_monuments/SNAPSHOT_2024_04_29_scheduled_monuments/",
)


# World Heritage Sites (WHS)
class HeWhsRaw(DataFrameModel):
    """Model for World Heritage Sites dataset.

    Attributes: 
       ListEntry: Reference id for each WHS
       Name: Name of the WHS
       geometry: Geospatial polygons in EPSG:27700
    """

    ListEntry: str = Field() 
    Name: str = Field()
    geometry: Geometry(crs=SRID) = Field() 


he_whs_raw = SourceDataset(
    name="he_whs_raw",
    medallion="bronze",
    source="he",
    model=HeWhsRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_world_heritage_sites/format_GEOPARQUET_world_heritage_sites/SNAPSHOT_2024_04_29_world_heritage_sites/",
)