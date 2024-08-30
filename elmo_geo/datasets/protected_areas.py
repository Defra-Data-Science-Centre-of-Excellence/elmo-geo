"""Protected Areas for Conservation identified using guidance written by the JNCC. Protected areas include:

- Sites of Special Scientific Interest (SSSI)
    Sites of Special Scientific Interest Site (SSSI) Units are divisions of SSSIs based on habitat,
    tenure and management, and are the basis for recording all information on SSSI Condition and management.
    A SSSI is the land notified as an SSSI under the Wildlife and Countryside Act (1981), as amended.
    SSSI are the finest sites for wildlife and natural features in England,
    supporting many characteristic, rare and endangered species, habitats and natural features.

- National Nature Reserves
    A National Nature Reserve (NNR) is the land declared under the National Parks and Access
    to the Countryside Act 1949 or Wildlife and Countryside Act (1981) as amended. The data does not include "proposed" sites. 

- Special Areas of Conservation (SAC)
    A Special Area of Conservation (SAC) is the land designated under Directive 92/43/EEC on the Conservation of
    Natural Habitats and of Wild Fauna and Flora. Data supplied has the status of "Candidate". The data does not include "proposed" Sites.

- Special Protection Area (SPAs)
    Special Protection Areas (SPAs) protect bird species that are dependent on the marine environment
    for all or part of their lifecycle, where these species are found in association with intertidal or subtidal habitats within the site.
    JNCC collates boundary and attribute data on Special Protection Areas (SPAs) on behalf of the
    Country Nature Conservation Bodies (CNCBs) to create a dataset of inshore and offshore sites across the UK. 

- Ramsar
    A Ramsar site is the land listed as a Wetland of International Importance under the Convention on Wetlands of International Importance
    Especially as Waterfowl Habitat (the Ramsar Convention) 1973.

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
from elmo_geo.etl.transformations import combine_wide, join_parcels

from .rpa_reference_parcels import reference_parcels


# Sites of Scientific Interest (SSSI)
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
    condition: str = Field(coerce=True, nullable=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


class NESSSIUnitsParcels(DataFrameModel):
    """Model for Natural England Sites of Special Scientific Interest (SSSI) dataset joined with Rural Payment Agency parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        proportion: The proportion of the parcel that intersects with the sssi units
    """

    id_parcel: str = Field(coerce=True)
    proportion: float = Field(ge=0, le=1)


ne_sssi_units_raw = SourceDataset(
    name="ne_sssi_units_raw",
    level0="bronze",
    level1="ne",
    model=NESSSIUnitsRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_sites_of_special_scientific_interest_units/format_GEOPARQUET_sites_of_special_scientific_interest_units/LATEST_sites_of_special_scientific_interest_units/Sites_of_Special_Scientific_Interest_Units_England.parquet",
)

ne_sssi_units_parcels = DerivedDataset(
    is_geo=False,
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


class NESSSINNRParcels(DataFrameModel):
    """Model for Natural England National Nature Reserves (NNR) dataset joined with Rural Payment Agency parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        proportion: The proportion of the parcel that intersects with the National Nature Reserves.
    """

    id_parcel: str = Field(coerce=True)
    proportion: float = Field(ge=0, le=1)


ne_nnr_raw = SourceDataset(
    name="ne_nnr_raw",
    level0="bronze",
    level1="ne",
    model=NENNRRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_nature_reserves/format_GEOPARQUET_national_nature_reserves/LATEST_national_nature_reserves/National_Nature_Reserves_England.parquet",
)


ne_nnr_parcels = DerivedDataset(
    is_geo=False,
    name="ne_nnr_parcels",
    level0="silver",
    level1="ne",
    restricted=False,
    func=join_parcels,
    dependencies=[reference_parcels, ne_nnr_raw],
    model=NESSSINNRParcels,
)


# Special Areas of Conservation (SAC)
class NESACRaw(DataFrameModel):
    """Model for Natural England Special Areas of Conservation (SAC) dataset.
    Attributes:
        sac_name: Name of each SAC
        sac_code: Reference id for each SAC
        geometry: Geospatial polygons in EPSG:27700
    """

    sac_name: str = Field(coerce=True)
    sac_code: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


class NESACParcels(DataFrameModel):
    """Model for Natural England Special Areas of Conservation (SAC) dataset joined with Rural Payment Agency parcel dataset.

    Parame"ters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        proportion: The proportion of the parcel that intersects with the Special Areas of Conservation.
    """

    id_parcel: str = Field(coerce=True)
    proportion: float = Field(ge=0, le=1)


ne_sac_raw = SourceDataset(
    name="ne_sac_raw",
    level0="bronze",
    level1="ne",
    model=NESACRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_special_areas_for_conservation/format_GEOPARQUET_special_areas_for_conservation/LATEST_special_areas_for_conservation/Special_Areas_of_Conservation_England.parquet",
)


ne_sac_parcels = DerivedDataset(
    is_geo=False,
    name="ne_sac_parcels",
    level0="silver",
    level1="ne",
    restricted=False,
    func=join_parcels,
    dependencies=[reference_parcels, ne_sac_raw],
    model=NESACParcels,
)


# Special Protection Areas (SPAs)
class JNCCSPARaw(DataFrameModel):
    """Model for Joint Nature Conservation Committee Special Protection Areas (SPAs) dataset.
    Attributes:
        spa_name: Name of each SPA
        spa_code: Reference id for each SPA
        geometry: Geospatial polygons in EPSG:27700
    """

    spa_name: str = Field(coerce=True)
    spa_code: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


class JNCCSPARParcels(DataFrameModel):
    """Model for Joint Nature Conservation Committee Special Protection Areas (SPAs) dataset joined with Rural Payment Agency parcel dataset.
    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        proportion: The proportion of the parcel that intersects with the Special Protection Areas.
    """

    id_parcel: str = Field(coerce=True)
    proportion: float = Field(ge=0, le=1)


jncc_spa_raw = SourceDataset(
    name="jncc_spa_raw",
    level0="bronze",
    level1="jncc",
    model=JNCCSPARaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_special_protection_areas/format_GEOPARQUET_special_protection_areas/LATEST_special_protection_areas/Special_Protection_Areas_England.parquet",
)


jncc_spa_parcels = DerivedDataset(
    is_geo=False,
    name="jncc_spa_parcels",
    level0="silver",
    level1="jncc",
    restricted=False,
    func=join_parcels,
    dependencies=[reference_parcels, jncc_spa_raw],
    model=JNCCSPARParcels,
)


# ramsar
class NERamsarRaw(DataFrameModel):
    """Model for Natural England Ramsar dataset.
    Attributes:
        name: Name of each ramsar site
        code: Reference id for each site
        geometry: Geospatial polygons in EPSG:27700
    """

    name: str = Field(coerce=True)
    code: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


class NERamsarParcels(DataFrameModel):
    """Model for Natural England Ramsar dataset joined with Rural Payment Agency parcel dataset.
    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        proportion: The proportion of the parcel that intersects with the Ramsar sites.
    """

    id_parcel: str = Field(coerce=True)
    proportion: float = Field(ge=0, le=1)


ne_ramsar_raw = SourceDataset(
    name="ne_ramsar_raw",
    level0="bronze",
    level1="ne",
    model=NERamsarRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_ramsar/format_GEOPARQUET_ramsar/LATEST_ramsar/Ramsar_England.parquet",
)


ne_ramsar_parcels = DerivedDataset(
    is_geo=False,
    name="ne_ramsar_parcels",
    level0="silver",
    level1="ne",
    restricted=False,
    func=join_parcels,
    dependencies=[reference_parcels, ne_ramsar_raw],
    model=NERamsarParcels,
)


# Marine Conservation Zones
class NEMarineConservationZonesRaw(DataFrameModel):
    """Model for Natural England Marine Conservation Zones (MCZ) dataset.
    Attributes:
        MCZ_NAME: Name of each MCZ site
        MCZ_CODE: Reference id for each MCZ
        geometry: Geospatial polygons in EPSG:27700
    """

    MCZ_NAME: str = Field(coerce=True)
    MCZ_CODE: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


class NEMarineConservationZonesParcels(DataFrameModel):
    """Model for Natural England Marine Conservation Zones (MCZ) dataset joined with Rural Payment Agency parcel dataset.
    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        proportion: The proportion of the parcel that intersects with MCZ sites.
    """

    id_parcel: str = Field(coerce=True)
    proportion: float = Field(ge=0, le=1)


ne_marine_conservation_zones_raw = SourceDataset(
    name="ne_marine_conservation_zones_raw",
    level0="bronze",
    level1="ne",
    model=NEMarineConservationZonesRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_natural_england_open_data_geoportal/dataset_marine_conservation_zones/format_GEOPARQUET_marine_conservation_zones/LATEST_marine_conservation_zones/Marine_Conservation_Zones___Natural_England_and_JNCC.parquet",
)


ne_marine_conservation_zones_parcels = DerivedDataset(
    is_geo=False,
    name="ne_marine_conservation_zones_parcels",
    level0="silver",
    level1="ne",
    restricted=False,
    func=join_parcels,
    dependencies=[reference_parcels, ne_marine_conservation_zones_raw],
    model=NEMarineConservationZonesParcels,
)


# Protected Areas One Big Table
class ProtectedAreasParcels(DataFrameModel):
    """Model for creating one big table that pulls together the proportion fields for each protected area dervived dataset linked to parcels.
    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        sssi: The proportion of the parcel that intersects with sssi sites.
        nnr: The proportion of the parcel that intersects with nnr sites.
        sac: The proportion of the parcel that intersects with sac sites.
        spa: The proportion of the parcel that intersects with spa sites.
        ramsar: The proportion of the parcel that intersects with ramsar sites.
        mcz: The proportion of the parcel that intersects with mcz sites."""

    id_parcel: str = Field(coerce=True)
    proportion_sssi: float = Field(ge=0, le=1)
    proportion_nnr: float = Field(ge=0, le=1)
    proportion_sac: float = Field(ge=0, le=1)
    proportion_spa: float = Field(ge=0, le=1)
    proportion_ramsar: float = Field(ge=0, le=1)
    proportion_mcz: float = Field(ge=0, le=1)


protected_areas_parcels = DerivedDataset(
    is_geo=False,
    name="protected_areas_parcels",
    level0="silver",
    level1="protected_areas_combined",
    restricted=False,
    func=partial(combine_wide, sources=["sssi", "nnr", "sac", "spa", "ramsar", "mcz"]),
    dependencies=[ne_sssi_units_parcels, ne_nnr_parcels, ne_sac_parcels, jncc_spa_parcels, ne_ramsar_parcels, ne_marine_conservation_zones_parcels],
    model=ProtectedAreasParcels,
)
