""" Protected Areas for Conservation identified using guidance written by the JNCC protected areas include:

- Sites of Special Scientific Interest (SSSI)
    Sites of Special Scientific Interest Site (SSSI) Units are divisions of SSSIs based on habitat,
    tenure and management, and are the basis for recording all information on SSSI Condition and management.
    A SSSI is the land notified as an SSSI under the Wildlife and Countryside Act (1981), as amended.
    SSSI are the finest sites for wildlife and natural features in England,
    supporting many characteristic, rare and endangered species, habitats and natural features.

- National Nature Reserves
    A National Nature Reserve (NNR) is the land declared under the National Parks and Access
    to the Countryside Act 1949 or Wildlife and Countryside Act (1981) as amended. The data does not include "proposed" sites. 

- Special areas of Conservation (SAC)
    A Special Area of Conservation (SAC) is the land designated under Directive 92/43/EEC on the Conservation of
    Natural Habitats and of Wild Fauna and Flora. Data supplied has the status of "Candidate". The data does not include "proposed" Sites.

- Special Protection Area (SPAs)
    Special Protection Areas (SPAs) with "marine components" protect bird species that are dependent on the marine environment
    for all or part of their lifecycle, where these species are found in association with intertidal or subtidal habitats within the site.
    JNCC collates boundary and attribute data on Special Protection Areas (SPAs) on behalf of the
    Country Nature Conservation Bodies (CNCBs) to create a dataset of inshore and offshore sites across the UK. 

- Ramsar
    A Ramsar site is the land listed as a Wetland of International Importance under the Convention on Wetlands of International Importance
    Especially as Waterfowl Habitat (the Ramsar Convention) 1973. Data supplied has the status of "Listed". The data does not include "proposed" sites.

- Marine Conservation Zones
    These are the boundaries for Marine Conservation Zones, and Highly Protected Marine Areas,
    which are both designated under the Marine and Coastal Access Act (2009).
    They protect nationally important marine wildlife, habitats, geology and geomorphology.
    Sites were selected in English inshore and offshore waters to protect not just rare and threatened marine wildlife and habitats,
    but also the range of biodiversity across biogeographic regions.

JNCC guidance available here: https://jncc.gov.uk/our-work/uk-protected-areas/"""

from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels

class NESSSIUnitsRaw(DataFrameModel):
    """Model for Natural England Sites of Special Sscientific Interest (SSSI) units dataset.
    Attributes:
       sssi_name: Name of the SSSI
       id: Reference id for the SSSI unit
       condition: Condition of the SSSI unit, the objective is for all to be in favourable condition
       geometry: Geospatial polygons in EPSG:27700
    """

    sssi_name: str = Field(coerce=True)
    id: float = Field(coerce=True)
    condition: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


class NESSSIUnitsParcels(DataFrameModel):
    """Model for Natural England Sites of Special Scientific Interest (SSSI) with parcel dataset.

    Parameters:
        sssi_name:Name of the SSSI
        id: Reference id for the SSSI unit
        condition: Condition of the SSSI unit, the objective is for all to be in favourable condition
        proportion: The proportion of the parcel that intersects with the sssi units
    """

    id_parcel: str: Field()
    proportion: float = Field(ge=0, le=1)


ne_sssi_units_raw = SourceDataset(
    is_geo=False,
    name="ne_sssi_units_raw",
    level0="bronze",
    level1="ne",
    model=NESSSIUnitsRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_sites_of_special_scientific_interest_units/format_GEOPARQUET_sites_of_special_scientific_interest_units/LATEST_sites_of_special_scientific_interest_units/Sites_of_Special_Scientific_Interest_Units_England.parquet",
)

ne_sssi_units_parcels = DerivedDataset(
    name="ne_sssi_units_parcels",
    level0="silver",
    level1="ne",
    restricted=False,
    func=join_parcels,
    dependencies=[reference_parcels, ne_sssi_units_raw],
    model=NESSSIUnitsParcels,
)


# National Naruture Reserves (NNR)
class NENNRRaw(DataFrameModel):
    """Model for Natural England National Nature Reserves (NNR) dataset.
    Attributes:
       nnr_name: Name of the NNR
       reference: Reference id for each NNR
       geometry: Geospatial polygons in EPSG:27700
    """

    nnr_name: str = Field(coerce=True)
    reference: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


ne_nnr_raw = SourceDataset(
    name="ne_nnr_raw",
    level0="bronze",
    level1="ne",
    model=NENNRRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_nature_reserves/format_GEOPARQUET_national_nature_reserves/LATEST_national_nature_reserves/National_Nature_Reserves_England.parquet",
)


# Special Areas of Conservation (SAC)
class NESACRaw(DataFrameModel):
    """Model for Natural England Special Areas of Conservation (SAC) dataset.
    Attributes:
        sac_name: Name of each SAC
        sac_code: Reference id for each SAC
        geometry: Geospatial polygons in EPSG:27700
    """

    sac_name: str = Field(coerce=True)  # check could be object?
    sac_code: str = Field(coerce=True)  # check could be object?
    geometry: Geometry(crs=SRID) = Field(coerce=True)


ne_sac_raw = SourceDataset(
    name="ne_sac_raw",
    level0="bronze",
    level1="ne",
    model=NESACRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_special_areas_for_conservation/format_GEOPARQUET_special_areas_for_conservation/LATEST_special_areas_for_conservation/Special_Areas_of_Conservation_England.parquet",
)


# Special Protection Areas (SPAs)
class JNCCSPARaw(DataFrameModel):
    """Model for Joint Nature Conservation Committee Special Protection Areas (SPAs) dataset.
    Attributes:
        spa_name: Name of each SPA
        spa_code: Reference id for each SPA
        geometry: Geospatial polygons in EPSG:27700
    """

    spa_name: str = Field(coerce=True)  # check could be object?
    spa_code: str = Field(coerce=True)  # check could be object?
    geometry: Geometry(crs=SRID) = Field(coerce=True)


jncc_spa_raw = SourceDataset(
    name="jncc_spa_raw",
    level0="bronze",
    level1="jncc",
    model=JNCCSPARaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_special_protection_areas/format_GEOPARQUET_special_protection_areas/LATEST_special_protection_areas/Special_Protection_Areas_England.parquet",
)


# ramsar
class NERamsarRaw(DataFrameModel):
    """Model for Natural England Ramsar dataset.
    Attributes:
        name: Name of each ramsar site
        code: Reference id for each site
        geometry: Geospatial polygons in EPSG:27700
    """

    name: str = Field(coerce=True)  # check could be object?
    code: str = Field(coerce=True)  # check could be object?
    geometry: Geometry(crs=SRID) = Field(coerce=True)


ne_ramsar_raw = SourceDataset(
    name="ne_ramsar_raw",
    level0="bronze",
    level1="ne",
    model=NERamsarRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_ramsar/format_GEOPARQUET_ramsar/LATEST_ramsar/Ramsar_England.parquet",
)


# Marine Conservation Zones
class NEMarineConservationZonesRaw(DataFrameModel):
    """Model for Natural England Marine Conservation Zones (MCZ) dataset.
    Attributes:
        MCZ_NAME: Name of each MCZ site
        MCZ_CODE: Reference id for each MCZ
        geometry: Geospatial polygons in EPSG:27700
    """

    MCZ_NAME: str = Field(coerce=True)  # check could be object?
    MCZ_CODE: str = Field(coerce=True)  # check could be object?
    geometry: Geometry(crs=SRID) = Field(coerce=True)


ne_marine_conservation_zones_raw = SourceDataset(
    name="ne_marine_conservation_zones_raw",
    level0="bronze",
    level1="ne",
    model=NEMarineConservationZonesRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_natural_england_open_data_geoportal/dataset_marine_conservation_zones/format_GEOPARQUET_marine_conservation_zones/LATEST_marine_conservation_zones/Marine_Conservation_Zones___Natural_England_and_JNCC.parquet",
)
