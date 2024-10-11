"""England Woodland Creation Offer (EWCO) datasets from Forestry Commission.

[A guide to Forestry Commission's sensitivity maps for woodland creation](https://www.gov.uk/guidance/a-guide-to-forestry-commissions-sensitivity-maps-for-woodland-creation)

The low sensitivity areas have fewest identified constraints to address,
and it should be easier to agree creating new woodland here than in other areas.
"""
from functools import partial

import geopandas as gpd
from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import sjoin_parcel_proportion

from .rpa_reference_parcels import reference_parcels

_join_parcels = partial(sjoin_parcel_proportion, columns=["spatial_priority"])


class EwcoClean(DataFrameModel):
    """Model describing the Forestry Commission's EWCO Nature Recovery Priority Habitat dataset.

    Attributes:
        geometry: The sensitivity classification's geospatial extent (polygons).
        spatial_priority: The spatial priority, one of `{'Premium', 'High', 'Lower'}`.
    """

    geometry: Geometry(crs=SRID) = Field()
    spatial_priority: str = Field(isin=["Premium", "High", "Lower"])


class SpatialPriorityParcels(DataFrameModel):
    """Model describing the EWCO Nature Recovery Priority Habitat parcel-level dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        spatial_priority: The spatial priority, one of `{'Premium', 'High', 'Lower'}`.
        proportion: The proportion of the parcel that intersects with the spatial priority.
    """

    id_parcel: str = Field()
    spatial_priority: str = Field()
    proportion: float = Field(ge=0, le=1)


def _clean_dataset(ds: Dataset) -> gpd.GeoDataFrame:
    """Only keep the geometry and simplify the cat column renaming to spatial_priority, explode geoms.

    `cat` column is renamed `spatial_priority`, and values are simplified to be one of {'Premium', 'High', 'Low'}.
    """
    return (
        ds.gdf(columns=["geometry", "cat"])
        .assign(spatial_priority=lambda df: df.cat.map(lambda x: x.split(" ")[0]).astype("category"))
        .drop(columns=["cat"])
        .explode()
    )


ewco_nature_recovery_priority_habitat_raw = SourceDataset(
    name="ewco_nature_recovery_priority_habitat_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/forestry_commission/ewco_nature_recovery_priority_habitat/EWCO___Nature_Recovery___Priority_Habitat_Network.shp",
)
"""Raw sourced version of Forestry Commission's EWCO Nature Recovery Priority Habitat dataset.

EWCO = England Woodland Creation Offer

Source:
    - [Forestry Commission Open Data](https://data-forestry.opendata.arcgis.com/datasets/346b5fcf70e54f4f8e072e621d350a7c_0)
"""

ewco_nature_recovery_priority_habitat = DerivedDataset(
    name="ewco_nature_recovery_priority_habitat",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    model=EwcoClean,
    func=_clean_dataset,
    dependencies=[ewco_nature_recovery_priority_habitat_raw],
)
"""Cleaned version of Forestry Commission's EWCO Nature Recovery Priority Habitat dataset."""

ewco_nature_recovery_priority_habitat_parcels = DerivedDataset(
    name="ewco_nature_recovery_priority_habitat_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=_join_parcels,
    dependencies=[reference_parcels, ewco_nature_recovery_priority_habitat],
    model=SpatialPriorityParcels,
    is_geo=False,
)
"""Definition for Forestry Commission's SFI Agroforestry dataset joined to RPA Parcels."""


# EWCO Red Squirrels
class EwcoRedSquirrelRaw(DataFrameModel):
    """Model describing the EWCO priority species red squirrel dataset.

    Attributes:
        sitename: name of the red squirrel reserve/stronghold and it’s buffer
        cat: title of the targeting use for this feature
        cswcm_pnts: scoring values relating to Countryside Stewardship schemes
        ewco_val:EWCO £ value the additional contribution provides per Ha if awarded
        geometry: polygons
    """

    sitename: str = Field()
    cat: str = Field()
    cswcm_pnts: str = Field()
    ewco_val: str = Field()
    geometry: Geometry(crs=SRID) = Field()


class EwcoRedSquirrelParcels(DataFrameModel):
    """Model describing the EWCO priority species red squirrel dataset joined with Rural Payment Agency parcel dataset

    Attributes:
    id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
    proportion: The proportion of the parcel that intersects with the red squiell areas
    """

    id_parcel: str = Field(unique=True)
    proportion: float = Field(ge=0, le=1)


ewco_red_squirrel_raw = SourceDataset(
    name="ewco_red_squirrel_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    model=EwcoRedSquirrelRaw,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/ewco/red_squirrel/2022_10_18/EWCO_Biodiversity___Priority_Species___Red_Squirrel___Woodland_Creation.shp",
)


ewco_red_squirrel_parcels = DerivedDataset(
    is_geo=False,
    name="ewco_red_squirrel_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=sjoin_parcel_proportion,
    dependencies=[reference_parcels, ewco_red_squirrel_raw],
    model=EwcoRedSquirrelParcels,
)
"""Spatial data supporting the England Woodland Creation Offer (EWCO) additional contribution targeting for Nature Recovery.
This layer is identical to that titled ‘CS WCM Biodiversity - Priority Species - Red Squirrel """


# EWCO NfC Social
class EwcoNfcSocialRaw(DataFrameModel):
    """Model describing the EWCO NfC Social dataset.

    Attributes:
    status: features assigned as ‘Meets social criteria’
    geometry: polygons
    """

    status: str = Field()
    geometry: Geometry(crs=SRID) = Field()


class EwcoNfcSocialParcels(DataFrameModel):
    """Model describing the EWCO NfC Social dataset joined with Rural Payment Agency parcel dataset

    Attributes:
    id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
    proportion: The proportion of the parcel that intersects with nfc social areas
    """

    id_parcel: str = Field(unique=True)
    proportion: float = Field(ge=0, le=1)


ewco_nfc_social_raw = SourceDataset(
    name="ewco_nfc_social_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    model=EwcoNfcSocialRaw,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/ewco/nfc_social/2022_03_14/EWCO___NfC_Social.shp",
)


ewco_nfc_social_parcels = DerivedDataset(
    is_geo=False,
    name="ewco_nfc_social_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=sjoin_parcel_proportion,
    dependencies=[reference_parcels, ewco_nfc_social_raw],
    model=EwcoNfcSocialParcels,
)

"""Spatial data supporting the England Woodland Creation Offer (EWCO)
‘Close to settlements’ Additional Contribution. This contribution is
available where woodland creation will provide social and environmental benefits by being close to people. """
