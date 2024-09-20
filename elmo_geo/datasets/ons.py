"""Office of National Statistics (ONS) Open GeoPortal Layers, provided by DASH.

This is a collection of official subdivisions of England[^ons_geographies], these are typically used to limit analysis to domains of interest or identify
regional bias.

| Dataset Name | Short Name | Domain | Notes |
| ------------ | ---------- | ------ | ----- |
| Countries[^country] | | UK, BFE | These are; England, Northern Ireland, Scotland, Wales. |
| International Territorial Level 1[^region] | ITL1 / Regions | UK, BFE | ITL1s are also known as "Regions"[^ons_geographies] | |
| International Territorial Level 2[^itl2] | ITL2 | UK, BGC | ITL2s are "counties and groups of counties"[^ons_geographies] | |
| International Territorial Level 3[^itl2] | ITL3 | UK | ITL3s are "counties and groups of unitary authorities"<br>**Currently not available on DASH.** |
| Counties and Unitary Authorities[^cua] | CUA | England, BFE |
| Local Authority Districts[^lad] | LAD | England, BFE |
| Wards[^ward] | | England, BFE |
| Built Up Areas[^bua] | BAU | Great Britain, BGG | BUAs are not administrative boundaries.<br>**BUAs are useful for defining "rural". |

| Resolution[^ons_geoportal] | Code | Description |
| ---------- | ---- | ----------- |
| Boundary Full Extent | BFE | Full resolution boundaries go to the Extent of the Realm (Low Water Mark) and are the most detailed of the boundaries. |
| Boundary Full Clipped | BFC | Full resolution boundaries that are clipped to the coastline (Mean High Water mark). |
| Boundary Generalised Clipped | BGC | Generalised to 20m and clipped to the coastline (Mean High Water mark) and more generalised than the BFE boundaries. |
| Boundary Super Generalised Clipped | BSC | Generalised to 200m and clipped to the coastline (Mean High Water mark). |
| Boundary Ultra Generalised Clipped | BUC | Generalised to 500m and clipped to the coastline (Mean High Water mark). |
| Boundary Grid Extent | BGE | Grid formed of equally sized cells which extend beyond the coastline. |
| Boundary Grid Generalised | BGG | Generalised 50m grid squares. |

[^DASH: ONS]: https://app.powerbi.com/groups/de0d7293-1d23-4194-869d-a4ff2ed2d169/reports/c8802134-4f3b-484e-bf14-1ed9f8881450?ctid=770a2450-0227-4c62-90c7-4e38537f1102&pbi_source=linkShare&bookmarkGuid=0132cbdc-349d-44d2-b2bd-6aa036b90421
[^ons_geographies]: https://www.ons.gov.uk/methodology/geography/ukgeographies/
[^ons_geographies_pdf]: https://www.arcgis.com/sharing/rest/content/items/0f1aa7078628466ea4429dfa8393d87d/data
[^ons_geoportal]: https://geoportal.statistics.gov.uk/
[^country]: https://geoportal.statistics.gov.uk/datasets/8295b10303ce46c982f62af3733b9405_0/
[^region]: https://geoportal.statistics.gov.uk/datasets/ba75af3e4a00492ca30945ded570b183_0/
[^itl2]: https://geoportal.statistics.gov.uk/datasets/6750ae0351c749c4b40b31e5740233a0_0/
[^itl3]: https://geoportal.statistics.gov.uk/datasets/c769b68ed2f34da7a936da425cf6d853_0/
[^cua]: https://geoportal.statistics.gov.uk/datasets/445118cc2e3b495aa81afa3925bfb0d9_0/
[^lad]: https://geoportal.statistics.gov.uk/datasets/3cba595dd06848f68879e3f2c3604f7e_0/
[^ward]: https://geoportal.statistics.gov.uk/datasets/3f28a3b7919b4a579b34c0823b386a51_0/
[^bua]: https://geoportal.statistics.gov.uk/datasets/53c633a774634385bb3ec5344f6bd4e2_0/
"""

from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels


# Countries
class CountryRaw(DataFrameModel):
    """Model for ONS Countries dataset.

    Parameters:
        fid: Is the country code, from 2023.
        name: Is the country name, from 2023.
        geometry: 4 MultiPolygons in EPSG:27700 at BFE.
    """

    fid: str = Field(coerce=True, unique=True, alias="CTRY23CD")
    name: str = Field(coerce=True, alias="CTRY23NM")
    geometry: Geometry(crs=SRID) = Field(coerce=True)


country_raw = SourceDataset(
    name="country_raw",
    level0="bronze",
    level1="ons",
    model=CountryRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_countries_bdy_uk_bfe/format_GEOPARQUET_countries_bdy_uk_bfe/LATEST_countries_bdy_uk_bfe/",
)


class CountryParcels(DataFrameModel):
    """Model for ONS Counties with parcel dataset.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        fid: Is the country code, from 2023.
        name: Is the country name, from 2023.
        proportion: The proportion of the parcel that intersects with feature.
    """

    id_parcel: str = Field()
    fid: str = Field()
    name: str = Field()
    proportion: float = Field(ge=0, le=1)


country_parcels = DerivedDataset(
    is_geo=False,
    name="country_parcels",
    level0="silver",
    level1="ons",
    restricted=False,
    func=partial(join_parcels, columns=["fid", "name"]),
    dependencies=[reference_parcels, country_raw],
    model=CountryParcels,
)


# ITL1/Regions
class RegionRaw(DataFrameModel):
    """Model for ONS Regions dataset.

    Parameters:
        fid: Is the region code, from 2023.
        name: Is the region name, from 2023.
        geometry: 9 (Multi)Polygons in EPSG:27700 at BFE.
    """

    fid: str = Field(coerce=True, unique=True, alias="RGN23CD")
    name: str = Field(coerce=True, alias="RGN23NM")
    geometry: Geometry(crs=SRID) = Field(coerce=True)


region_raw = SourceDataset(
    name="region_raw",
    level0="bronze",
    level1="ons",
    model=RegionRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_regions_bdy_en_bfe/format_GEOPARQUET_regions_bdy_en_bfe/LATEST_regions_bdy_en_bfe/",
)


class RegionParcels(DataFrameModel):
    """Model for ONS Regions with parcel dataset.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        fid: Is the region code, from 2023.
        name: Is the region name, from 2023.
        proportion: The proportion of the parcel that intersects with feature.
    """

    id_parcel: str = Field()
    fid: str = Field()
    name: str = Field()
    proportion: float = Field(ge=0, le=1)


region_parcels = DerivedDataset(
    is_geo=False,
    name="region_parcels",
    level0="silver",
    level1="ons",
    restricted=False,
    func=partial(join_parcels, columns=["fid", "name"]),
    dependencies=[reference_parcels, region_raw],
    model=RegionParcels,
)


# ITL2
class ITL2Raw(DataFrameModel):
    """Model for ONS ITL2 (counties and groups of counties) dataset.

    Parameters:
        fid: Reference unique id for each geographic area ie TLC1, from 2021.
        name: Name of the county or group of counties ie Tees Valley and Durham, from 2021.
        geometry: MultiPolygons in EPSG:27700 at BGC.
    """

    fid: str = Field(coerce=True, unique=True, alias="ITL221CD")
    name: str = Field(coerce=True, alias="ITL221NM")
    geometry: Geometry(crs=SRID) = Field(coerce=True)


itl2_raw = SourceDataset(
    name="itl2_raw",
    level0="bronze",
    level1="ons",
    model=ITL2Raw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_int_territorial_lvl2_2021_uk_bgc_v2/format_GPKG_int_territorial_lvl2_2021_uk_bgc_v2/LATEST_int_territorial_lvl2_2021_uk_bgc_v2/International_Territorial_Level_2_January_2021_UK_BGC_V2_2022_1205324512979248673.gpkg",
)


class ITL2Parcels(DataFrameModel):
    """Model for ONS ITL2 with parcel dataset.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        fid: Reference unique id for each geographic area ie TLC1.
        name: Name of the county or group of counties ie Tees Valley and Durham
        proportion: The proportion of the parcel that intersects with the itl2 boundary.
    """

    id_parcel: str = Field()
    fid: str = Field()
    name: str = Field()
    proportion: float = Field(ge=0, le=1)


itl2_parcels = DerivedDataset(
    is_geo=False,
    name="itl2_parcels",
    level0="silver",
    level1="ons",
    restricted=False,
    func=partial(join_parcels, columns=["fid", "name"]),
    dependencies=[reference_parcels, itl2_raw],
    model=ITL2Parcels,
)


# Counties and Unitary Authorities (CAUs)
class CUARaw(DataFrameModel):
    """Model for ONS Counties and Unitary Authorities (CUA) dataset.

    Parameters:
        fid: Is the CUA code, from 2023.
        name: Is the CUA name, from 2023.
        geometry: (Multi)Polygons in EPSG:27700 at BFE.
    """

    fid: str = Field(coerce=True, unique=True, alias="CTYUA23CD")
    name: str = Field(coerce=True, alias="CTYUA23NM")
    geometry: Geometry(crs=SRID) = Field(coerce=True)


cua_raw = SourceDataset(
    name="cua_raw",
    level0="bronze",
    level1="ons",
    model=CUARaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_counties_uni_authorities_bdy_uk_bfe/format_GEOPARQUET_counties_uni_authorities_bdy_uk_bfe/LATEST_counties_uni_authorities_bdy_uk_bfe/",
)


class CUAParcels(DataFrameModel):
    """Model for ONS CUA with parcel dataset.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        fid: Is the CUA code, from 2023.
        name: Is the CUA name, from 2023.
        proportion: The proportion of the parcel that intersects with feature.
    """

    id_parcel: str = Field()
    fid: str = Field()
    name: str = Field()
    proportion: float = Field(ge=0, le=1)


cua_parcels = DerivedDataset(
    is_geo=False,
    name="cua_parcels",
    level0="silver",
    level1="ons",
    restricted=False,
    func=partial(join_parcels, columns=["fid", "name"]),
    dependencies=[reference_parcels, cua_raw],
    model=CUAParcels,
)


# Local Authority Districts (LADs)
class LADRaw(DataFrameModel):
    """Model for ONS Local Authority Districts (LADs) dataset.

    Parameters:
        fid: Is the LAD code, from 2023.
        name: Is the LAD name, from 2023.
        geometry: (Multi)Polygons in EPSG:27700 at BFE.
    """

    fid: str = Field(coerce=True, unique=True, alias="LAD23CD")
    name: str = Field(coerce=True, alias="LAD23NM")
    geometry: Geometry(crs=SRID) = Field(coerce=True)


lad_raw = SourceDataset(
    name="lad_raw",
    level0="bronze",
    level1="ons",
    model=LADRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_local_authority_districts_bdy_uk_bfe/format_GEOPARQUET_local_authority_districts_bdy_uk_bfe/LATEST_local_authority_districts_bdy_uk_bfe/",
)


class LADParcels(DataFrameModel):
    """Model for ONS LAD with parcel dataset.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        fid: Is the LAD code, from 2023.
        name: Is the LAD name, from 2023.
        proportion: The proportion of the parcel that intersects with feature.
    """

    id_parcel: str = Field()
    fid: str = Field()
    name: str = Field()
    proportion: float = Field(ge=0, le=1)


lad_parcels = DerivedDataset(
    is_geo=False,
    name="lad_parcels",
    level0="silver",
    level1="ons",
    restricted=False,
    func=partial(join_parcels, columns=["fid", "name"]),
    dependencies=[reference_parcels, lad_raw],
    model=LADParcels,
)


# Wards
class WardRaw(DataFrameModel):
    """Model for ONS Wards dataset.

    Parameters:
        fid: Is the Ward code, from 2023.
        name: Is the Ward name, from 2023.
        geometry: (Multi)Polygons in EPSG:27700 at BFE.
    """

    fid: str = Field(coerce=True, unique=True, alias="WD23CD")
    name: str = Field(coerce=True, alias="WD23NM")
    geometry: Geometry(crs=SRID) = Field(coerce=True)


ward_raw = SourceDataset(
    name="ward_raw",
    level0="bronze",
    level1="ons",
    model=WardRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_wards_bdy_uk_bfe/format_GEOPARQUET_wards_bdy_uk_bfe/LATEST_wards_bdy_uk_bfe/",
)


class WardParcels(DataFrameModel):
    """Model for ONS Wards with parcel dataset.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        fid: Is the Ward code, from 2023.
        name: Is the Ward name, from 2023.
        proportion: The proportion of the parcel that intersects with feature.
    """

    id_parcel: str = Field()
    fid: str = Field()
    name: str = Field()
    proportion: float = Field(ge=0, le=1)


ward_parcels = DerivedDataset(
    is_geo=False,
    name="ward_parcels",
    level0="silver",
    level1="ons",
    restricted=False,
    func=partial(join_parcels, columns=["fid", "name"]),
    dependencies=[reference_parcels, ward_raw],
    model=WardParcels,
)


# Built Up Areas (BUA)
class BUARaw(DataFrameModel):
    """Model for ONS Built Up Areas (BUA) dataset.

    Parameters:
        fid: Is the BUA code, from 2022.
        name: Is the BUA name, from 2022.
        geometry: (Multi)Polygons in EPSG:27700 at BGG.
    """

    fid: str = Field(coerce=True, unique=True, alias="BUA22CD")
    name: str = Field(coerce=True, alias="BUA22NM")
    geometry: Geometry(crs=SRID) = Field(coerce=True)


bua_raw = SourceDataset(
    name="bua_raw",
    level0="bronze",
    level1="ons",
    model=BUARaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_built_up_areas_2022_gb_bgg/format_GEOPARQUET_built_up_areas_2022_gb_bgg/LATEST_built_up_areas_2022_gb_bgg",
)


class BUAParcels(DataFrameModel):
    """Model for ONS Built Up Areas (BUA) with parcel dataset.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        fid: Is the BUA code, from 2022.
        name: Is the BUA name, from 2022.
        proportion: The proportion of the parcel that intersects with feature.
    """

    id_parcel: str = Field()
    fid: str = Field()
    name: str = Field()
    proportion: float = Field(ge=0, le=1)


bua_parcels = DerivedDataset(
    is_geo=False,
    name="bua_parcels",
    level0="silver",
    level1="ons",
    restricted=False,
    func=partial(join_parcels, columns=["fid", "name"]),
    dependencies=[reference_parcels, bua_raw],
    model=BUAParcels,
)
