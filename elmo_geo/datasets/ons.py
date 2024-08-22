"""Office of National Statistics (ONS) Open GeoPortal Layers, provided by DASH.

This is a collection of official subdivisions of England, these are typically used to limit analysis to domains of interest or identify regional bias.

| Dataset Name | Short Name | Domain | Similar | Example |
| ------------ | ---------- | ------ | ------- | ------- |
| [International Territorial Level 1][^itl1] | ITL1 | UK | Regions |
| [International Territorial Level 2][^itl2] | ITL2 | UK |
| [International Territorial Level 3][^itl2] | ITL3 | UK |
| [Countries][^country] | | UK | | England, Northern Ireland, Scotland, Wales |
| [Councils][^council] | | UK |
| [Counties and Unitary Authorities][^cau] | CAU | England |
| [Local Authority Districts][^lad] | LAD | England |
| [Wards][^ward] | | England |
| [Parishes][^parish] | | England |

/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_countries_bdy_uk_bfe/format_GEOPARQUET_countries_bdy_uk_bfe/LATEST_countries_bdy_uk_bfe/
/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_regions_bdy_en_bfe/format_GEOPARQUET_regions_bdy_en_bfe/LATEST_regions_bdy_en_bfe/
/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_counties_uni_authorities_bdy_uk_bfe/format_GEOPARQUET_counties_uni_authorities_bdy_uk_bfe/LATEST_counties_uni_authorities_bdy_uk_bfe/
/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_local_authority_districts_bdy_uk_bfe/format_GEOPARQUET_local_authority_districts_bdy_uk_bfe/LATEST_local_authority_districts_bdy_uk_bfe/
/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_wards_bdy_uk_bfe/format_GEOPARQUET_wards_bdy_uk_bfe/LATEST_wards_bdy_uk_bfe/

[^itl1]:
[^itl2]: https://geoportal.statistics.gov.uk/datasets/6750ae0351c749c4b40b31e5740233a0_0/explore?location=54.959130%2C-3.316600%2C6.04
[^itl3]:
[^country]:
[^council]:
[^cau]:
[^lad]:
[^ward]:
[^parish]:
"""

from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels


class ITL2BoundariesRaw(DataFrameModel):
    """Model for ONS ITL2 (counties and groups of counties) dataset.

    Parameters:
        ITL221CD: Reference unique id for each geographic area ie TLC1.
        ITL221NM: Name of the county or group of counties ie Tees Valley and Durham
        geometry: The ITL geospatial polygons are in EPSG:27700.
    """

    ITL221CD: str = Field(coerce=True)
    ITL221NM: str = Field(coerce=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


itl2_boundaries_raw = SourceDataset(
    name="itl2_boundaries_raw",
    level0="bronze",
    level1="ons",
    model=ITL2BoundariesRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_ons_open_geography_portal/dataset_int_territorial_lvl2_2021_uk_bgc_v2/format_GPKG_int_territorial_lvl2_2021_uk_bgc_v2/LATEST_int_territorial_lvl2_2021_uk_bgc_v2/International_Territorial_Level_2_January_2021_UK_BGC_V2_2022_1205324512979248673.gpkg",
)


class ITL2BoundariesParcels(DataFrameModel):
    """Model for ONS ITL2 with parcel dataset.

    Parameters:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        ITL221CD: Reference unique id for each geographic area ie TLC1.
        ITL221NM: Name of the county or group of counties ie Tees Valley and Durham
        proportion: The proportion of the parcel that intersects with the itl2 boundary.
    """

    id_parcel: str = Field()
    ITL221CD: str = Field()
    ITL221NM: str = Field()
    proportion: float = Field(ge=0, le=1)


itl2_boundaries_parcels = DerivedDataset(
    name="itl2_boundaries_parcels",
    level0="silver",
    level1="ons",
    restricted=False,
    func=partial(join_parcels, columns=["ITL221NM", "ITL221CD"]),
    dependencies=[reference_parcels, itl2_boundaries_raw],
    model=ITL2BoundariesParcels,
)
