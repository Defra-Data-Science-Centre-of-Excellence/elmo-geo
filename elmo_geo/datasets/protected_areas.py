"""Protected Areas[^gov][^jncc] and sites for conservation.

- Special Protection Area (SPAs)
    Special Protection Areas (SPAs) protect bird species that are dependent on the marine environment
    for all or part of their lifecycle, where these species are found in association with intertidal or subtidal habitats within the site.
    JNCC collates boundary and attribute data on Special Protection Areas (SPAs) on behalf of the
    Country Nature Conservation Bodies (CNCBs) to create a dataset of inshore and offshore sites across the UK. 

- Marine Conservation Zones
    These are the boundaries for Marine Conservation Zones, and Highly Protected Marine Areas,
    which are both designated under the Marine and Coastal Access Act (2009).
    They protect nationally important marine wildlife, habitats, geology and geomorphology.
    Sites were selected in English inshore and offshore waters to protect not just rare and threatened marine wildlife and habitats,
    but also the range of biodiversity across biogeographic regions.

- National Nature Reserves
    A National Nature Reserve (NNR) is the land declared under the National Parks and Access
    to the Countryside Act 1949 or Wildlife and Countryside Act (1981) as amended. The data does not include "proposed" sites. 

- Ramsar
    A Ramsar site is the land listed as a Wetland of International Importance under the Convention on Wetlands of International Importance
    Especially as Waterfowl Habitat (the Ramsar Convention) 1973.

- Special Areas of Conservation (SAC)
    A Special Area of Conservation (SAC) is the land designated under Directive 92/43/EEC on the Conservation of
    Natural Habitats and of Wild Fauna and Flora. Data supplied has the status of "Candidate". The data does not include "proposed" Sites.

- Sites of Special Scientific Interest (SSSI)
    Sites of Special Scientific Interest Site (SSSI) Units are divisions of SSSIs based on habitat,
    tenure and management, and are the basis for recording all information on SSSI Condition and management.
    A SSSI is the land notified as an SSSI under the Wildlife and Countryside Act (1981), as amended.
    SSSI are the finest sites for wildlife and natural features in England,
    supporting many characteristic, rare and endangered species, habitats and natural features.

[^jncc]: https://jncc.gov.uk/our-work/uk-protected-areas/
[^gov]: https://www.gov.uk/guidance/protected-sites-and-areas-how-to-review-planning-applications
"""
from functools import reduce

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry
from pyspark.sql import functions as F

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import sjoin_parcel_proportion
from elmo_geo.utils.types import SparkDataFrame

from .rpa_reference_parcels import reference_parcels


# Special Protection Areas (SPAs)
class JnccSpaRaw(DataFrameModel):
    """Model for Joint Nature Conservation Committee Special Protection Areas (SPAs) dataset.
    Attributes:
        name: Name of each SPA
        code: Reference id for each SPA
        geometry: Geospatial polygons in EPSG:27700
    """

    name: str = Field(alias="spa_name")
    code: str = Field(alias="spa_code")
    geometry: Geometry(crs=SRID) = Field()


jncc_spa_raw = SourceDataset(
    name="jncc_spa_raw",
    medallion="bronze",
    source="jncc",
    model=JnccSpaRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_special_protection_areas/format_GEOPARQUET_special_protection_areas/LATEST_special_protection_areas/",
)


# Marine Conservation Zones
class NeMczRaw(DataFrameModel):
    """Model for Natural England Marine Conservation Zones (MCZ) dataset.
    Attributes:
        name: Name of each MCZ site
        code: Reference id for each MCZ
        geometry: Geospatial polygons in EPSG:27700
    """

    name: str = Field(alias="MCZ_NAME")
    code: str = Field(alias="MCZ_CODE")
    geometry: Geometry(crs=SRID) = Field()


ne_mcz_raw = SourceDataset(
    name="ne_marine_conservation_zones_raw",
    medallion="bronze",
    source="ne",
    model=NeMczRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_natural_england_open_data_geoportal/dataset_marine_conservation_zones/format_GEOPARQUET_marine_conservation_zones/LATEST_marine_conservation_zones/",
)


# National Nature Reserves (NNR)
class NeNnrRaw(DataFrameModel):
    """Model for Natural England National Nature Reserves (NNR) dataset.
    Attributes:
       name: Name of the NNR
       code: Reference id for each NNR
       geometry: Geospatial polygons in EPSG:27700
    """

    name: str = Field(alias="nnr_name")
    code: str = Field(alias="reference")
    geometry: Geometry(crs=SRID) = Field()


ne_nnr_raw = SourceDataset(
    name="ne_nnr_raw",
    medallion="bronze",
    source="ne",
    model=NeNnrRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_nature_reserves/format_GEOPARQUET_national_nature_reserves/LATEST_national_nature_reserves/",
)


# Ramsar
class NeRamsarRaw(DataFrameModel):
    """Model for Natural England Ramsar dataset.
    Attributes:
        name: Name of each ramsar site
        code: Reference id for each site
        geometry: Geospatial polygons in EPSG:27700
    """

    name: str = Field()
    code: str = Field()
    geometry: Geometry(crs=SRID) = Field()


ne_ramsar_raw = SourceDataset(
    name="ne_ramsar_raw",
    medallion="bronze",
    source="ne",
    model=NeRamsarRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_ramsar/format_GEOPARQUET_ramsar/LATEST_ramsar/",
)


# Special Areas of Conservation (SAC)
class NeSacRaw(DataFrameModel):
    """Model for Natural England Special Areas of Conservation (SAC) dataset.
    Attributes:
        name: Name of each SAC
        code: Reference id for each SAC
        geometry: Geospatial polygons in EPSG:27700
    """

    name: str = Field(alias="sac_name")
    code: str = Field(alias="sac_code")
    geometry: Geometry(crs=SRID) = Field()


ne_sac_raw = SourceDataset(
    name="ne_sac_raw",
    medallion="bronze",
    source="ne",
    model=NeSacRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_special_areas_for_conservation/format_GEOPARQUET_special_areas_for_conservation/LATEST_special_areas_for_conservation/",
)


# Sites of Scientific Interest (SSSIs)
class NeSssiUnitsRaw(DataFrameModel):
    """Model for Natural England Sites of Special Scientific Interest (SSSI) units dataset.
    Attributes:
       name: Name of the SSSI
       code: Reference id for the SSSI unit
       condition: Condition of the SSSI unit, the objective is for all to be in favourable condition
       geometry: Geospatial polygons in EPSG:27700
    """

    name: str = Field(alias="sssi_name")
    code: float = Field(alias="id")
    condition: str = Field(nullable=True)
    geometry: Geometry(crs=SRID) = Field()


ne_sssi_units_raw = SourceDataset(
    name="ne_sssi_units_raw",
    medallion="bronze",
    source="ne",
    model=NeSssiUnitsRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_sites_of_special_scientific_interest_units/format_GEOPARQUET_sites_of_special_scientific_interest_units/LATEST_sites_of_special_scientific_interest_units/",
)


# Protected Areas Tidy
class ProtectedAreasTidy(DataFrameModel):
    """Model for one wide table that pulls together the proportion fields for each protected area derived dataset linked to parcels.

    Attributes:
        source: Type of Protected Area, e.g. SPA/MCZ/NNR/Ramsar/SAC/SSSI.
        name: Name of the specific Protected Area.
        code: Code/reference/id for the specific Protected Area.
        geometry: BNG polygon of the Protected Area.
    """
    source: str = Field()
    name: str = Field()
    code: str = Field()
    geometry: Geometry(crs=SRID) = Field()


def _transform(*datasets: Dataset) -> SparkDataFrame:
    """Similar to `combine_long` and addition select only relevent columns.
    """
    sources = ["spa", "mcz", "nnr", "ramsar", "sac", "sssi"]
    return reduce(
        SparkDataFrame.unionByName,
        [
            dataset.sdf().selectExpr(f"'{source}' AS source", "name", "code", "geometry")
            for source, dataset in zip(sources, datasets)
        ],
    )

protected_areas_tidy = DerivedDataset(
    is_geo=True,
    name="protected_areas_tidy",
    medallion="silver",
    source="elmo_geo",
    restricted=False,
    func=_transform,
    dependencies=[
        jncc_spa_raw,
        ne_mcz_raw,
        ne_nnr_raw,
        ne_ramsar_raw,
        ne_sac_raw,
        ne_sssi_units_raw,
    ],
    model=ProtectedAreasTidy,
)


# Protected Areas Parcels
class ProtectedAreasParcels(DataFrameModel):
    """Model for one wide table that pulls together the proportion fields for each protected area derived dataset linked to parcels.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        proportion_any: The proportion of the parcel that intersects with any Protected Area.
        proportion_spa: The proportion of the parcel that intersects with SPA sites.
        proportion_mcz: The proportion of the parcel that intersects with MCZ sites.
        proportion_nnr: The proportion of the parcel that intersects with NNR sites.
        proportion_ramsar: The proportion of the parcel that intersects with Ramsar sites.
        proportion_sac: The proportion of the parcel that intersects with SAC sites.
        proportion_sssi: The proportion of the parcel that intersects with SSSI sites.
    """

    id_parcel: str = Field(unique=True)
    proportion_any: float = Field(ge=0, le=1)
    proportion_spa: float = Field(ge=0, le=1)
    proportion_mcz: float = Field(ge=0, le=1)
    proportion_nnr: float = Field(ge=0, le=1)
    proportion_ramsar: float = Field(ge=0, le=1)
    proportion_sac: float = Field(ge=0, le=1)
    proportion_sssi: float = Field(ge=0, le=1)


def _transform(reference_parcels: Dataset, protected_areas_tidy: Dataset) -> SparkDataFrame:
    """This is 2 `sjoin_parcel_proportion`s with a pivot,
    Modified to groupby source and groupby none, so "any" Protected Area is additionally available.
    """
    sdf_parcels = reference_parcels.sdf()
    sdf_pa = protected_areas_tidy.sdf()
    return (
        SparkDataFrame.join(
            sjoin_parcel_proportion(sdf_parcels, sdf_pa, columns=["source"]),
            sjoin_parcel_proportion(sdf_parcels, sdf_pa).withColumn("source", F.lit("any")),
            on = "id_parcel",
            how = "outer",
        )
        .groupby("id_parcel")
        .pivot("source")
        .sum("proportion")
        .withColumnsRenamed({
            "any": "proportion_any",
            "spa": "proportion_spa",
            "mcz": "proportion_mcz",
            "nnr": "proportion_nnr",
            "ramsar": "proportion_ramsar",
            "sac": "proportion_sac",
            "sssi": "proportion_sssi",
        })
    )

protected_areas_parcels = DerivedDataset(
    is_geo=False,
    name="protected_areas_parcels",
    medallion="gold",
    source="elmo_geo",
    restricted=False,
    func=sjoin_parcel_proportion,
    dependencies=[reference_parcels, protected_areas_tidy],
    model=ProtectedAreasParcels,
)
