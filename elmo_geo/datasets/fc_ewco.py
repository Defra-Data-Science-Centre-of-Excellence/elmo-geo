"""England Woodland Creation Offer (EWCO) datasets from Forestry Commission.

[A guide to Forestry Commission's sensitivity maps for woodland creation]
(https://www.gov.uk/guidance/a-guide-to-forestry-commissions-sensitivity-maps-for-woodland-creation)

The low sensitivity areas have fewest identified constraints to address,
and it should be easier to agree creating new woodland here than in other areas.
"""
from functools import partial

import geopandas as gpd
from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import combine_wide, sjoin_parcel_proportion

from .rpa_reference_parcels import reference_parcels

_join_parcels = partial(sjoin_parcel_proportion, columns=["spatial_priority"])


class EwcoParcels(DataFrameModel):
   """Model describing the parcel-level dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        proportion: The proportion of the parcel that intersects with the spatial priority.
    """

    id_parcel: str = Field(unique=True)
    proportion: float = Field(ge=0, le=1) 


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


# EWCO nfc_ammonia_emmissions
class EwcoAmmoniaEmmesionsRaw(DataFrameModel):
    """Model describing the EWCO NfC Ammonia Emissions Capture for SSSI Protection dataset.

    Attributes:
        status: features assigned as ‘Meets air quality criteria’
        pnts: point value awarded to applications Attribution statement
        geometry: polygons
    """

    status: str = Field()
    pnts: str = Field()
    geometry: Geometry(crs=SRID) = Field()


ewco_ammonia_emmesions_raw = SourceDataset(
    name="ewco_ammonia_emmesions_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    model=EwcoAmmoniaEmmesionsRaw,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/ewco/nfc_ammonia_emmissions/2022_03_14/EWCO___NfC_Ammonia_Emissions_Capture_for_SSSI_Protection.shp",
)


ewco_ammonia_emmesions_parcels = DerivedDataset(
    is_geo=False,
    name="ewco_ammonia_emmesions_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=sjoin_parcel_proportion,
    dependencies=[reference_parcels, ewco_ammonia_emmesions_raw],
    model=EwcoParcels,
)
"""Spatial data supporting the England Woodland Creation Offer (EWCO) additional point scoring for ammonia capture.
There is no Additional Contribution for ammonia capture but EWCO supports action to address air pollution.
Additional points are available for creating shelterbelts designed to capture ammonia emissions from farm sources in
locations where there is a potential risk of air pollution impacting a Site of Special Scientific Interest (SSSI)
– where sensitive habitats or species could be impacted by direct toxic effects of ammonia, nitrogen deposition or
acidification from ammonia emissions."""


# EWCO flood risk management
class EwcoFloodRiskRaw(DataFrameModel):
    """Model describing the EWCO Flood Risk Management dataset.

    Attributes:
        LANDSCAPE: the targeting category: Opportunity for Floodplain Woodland / Opportunity for Wider Catchment Woodland
        AreaHa: ‘AreaHa’ – Area of the feature in hectares
        geometry: polygons
    """

    LANDSCAPE: str = Field()
    AreaHa: float = Field()
    geometry: Geometry(crs=SRID) = Field()


ewco_flood_risk_raw = SourceDataset(
    name="ewco_flood_risk_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    model=EwcoFloodRiskRaw,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/ewco/flood_risk_management/2023_02_24/EWCO___Flood_Risk_Management.shp",
)


ewco_flood_risk_parcels = DerivedDataset(
    is_geo=False,
    name="ewco_flood_risk_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=sjoin_parcel_proportion,
    dependencies=[reference_parcels, ewco_flood_risk_raw],
    model=EwcoParcels,
)
"""Spatial data supporting appropriately located and designed woodland creation to help reduce flood risk by slowing
flood flows and increasing the retention and infiltration of water on the land.
The layer shows where the proportion of ‘Opportunity for Floodplain’ woodland creation and ‘Opportunity for Wider Catchment’"""


# EWCO Red Squirrels
class EwcoRedSquirrelRaw(DataFrameModel):
    """Model describing the EWCO priority species red squirrel dataset.

    Attributes:
        sitename: name of the red squirrel reserve/stronghold and it’s buffer
        cat: title of the targeting used for this feature
        cswcm_pnts: scoring values relating to Countryside Stewardship schemes
        ewco_val:EWCO £ value the additional contribution provides per Ha if awarded
        geometry: polygons
    """

    sitename: str = Field()
    cat: str = Field()
    cswcm_pnts: str = Field()
    ewco_val: str = Field()
    geometry: Geometry(crs=SRID) = Field()


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
    model=EwcoParcels,
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
    model=EwcoParcels,
)

"""Spatial data supporting the England Woodland Creation Offer (EWCO)
‘Close to settlements’ Additional Contribution. This contribution is
available where woodland creation will provide social and environmental benefits by being close to people. """


# Keeping Rivers Cool
class EwcoKeepingRiversCoolRaw(DataFrameModel):
    """Model describing the EWCO keeping rivers cool riparian buffers dataset.

    Attributes:
        AreaHa:area of the feature in hectares
        geometry:polygons
    """

    areaha: float = Field()
    geometry: Geometry(crs=SRID) = Field()


ewco_keeping_rivers_cool_raw = SourceDataset(
    name="ewco_keeping_rivers_cool_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    model=EwcoKeepingRiversCoolRaw,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/ewco/keeping_rivers_cool_riparian_buffers/2023_03_03/EWCO___Keeping_Rivers_Cool_Riparian_Buffers.shp",
)


ewco_keeping_rivers_cool_parcels = DerivedDataset(
    is_geo=False,
    name="ewco_keeping_rivers_cool_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=sjoin_parcel_proportion,
    dependencies=[reference_parcels, ewco_keeping_rivers_cool_raw],
    model=EwcoParcels,
)

"""Spatial data supporting appropriately located and designed woodland creation where this will provide dappled shade to improve aquatic
ecology by reducing summer water temperatures and benefiting wildlife dispersal (for example, otter) along the corridors of habitat this
creates. The data represents a 50 m buffer around patches of surface waterbodies (rivers) with little or no existing riparian shade."""


# priority habitat network
class EwcoPriorityHabitatNetworkRaw(DataFrameModel):
    """Model describing the EWCO Biodiversity Priority Habitat Network dataset.

    Attributes:
    cat: ‘Higher’ and ‘Lower’ priority area for woodland network expansion
    csht_pnts: base scoring value for Countryside Stewardship Higher Tier
    cswc_mpnts: scoring value per Ha for Countryside Stewardship woodland creation
    ewco_val: £ value the additional contribution provides per Ha if awarded
    geometry:polygons
    """

    cat: str = Field()
    csht_pnts: str = Field()
    cswc_mpnts: str = Field()
    ewco_val: str = Field()
    geometry: Geometry(crs=SRID) = Field()


ewco_priority_habitat_network_raw = SourceDataset(
    name="ewco_priority_habitat_network_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    model=EwcoPriorityHabitatNetworkRaw,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/ewco/priority_habitat_network/2022_10_06/EWCO_Biodiversity___Priority_Habitat_Network.shp",
)


ewco_priority_habitat_network_parcels = DerivedDataset(
    is_geo=False,
    name="ewco_priority_habitat_network_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=sjoin_parcel_proportion,
    dependencies=[reference_parcels, ewco_priority_habitat_network_raw],
    model=EwcoParcels,
)

"""Spatial data supporting the England Woodland Creation Offer (EWCO) additional contribution targeting for Nature Recovery, this layer
indicates the proportion of ‘High Spatial Priority’, ‘Higher’ and ‘Lower’ priority areas for woodland network expansion. 
"""


# Water Quality
class EwcoWaterQualityRaw(DataFrameModel):
    """Model describing the EWCO water quality dataset.

    Attributes:
    cat: the targeting category
    areaha: Area of the feature in hectares
    geometry: polygons
    """

    cat: str = Field()
    areaha: float = Field()
    geometry: Geometry(crs=SRID) = Field()


ewco_waterquality_raw = SourceDataset(
    name="ewco_waterquality_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    model=EwcoWaterQualityRaw,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/ewco/water_quality/2023_02_27/EWCO__E2_80_93_Water_Quality.shp",
)


ewco_waterquality_parcels = DerivedDataset(
    name="ewco_waterquality_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=sjoin_parcel_proportion,
    dependencies=[reference_parcels, ewco_waterquality_raw],
    model=EwcoParcels,
)

"""Spatial data supporting appropriately located and designed woodland creation to help reduce pollutants through land use
change that reduces fertilizer application or by creating woodland that intercepts pollution and sediment before it reaches watercourses.
"""


# Woodland Sensativity
class EwcoSensativityRaw(DataFrameModel):
    """Model describing the EWCO woodland sensativity dataset.

    Attributes:
    sensitivity: The sensitivity to woodland creation level the land has been assigned.
    areaha: the area in hectares of the polygon.
    geometry: polygons
    """

    sensitivit: str = Field()
    areaha: float = Field()
    geometry: Geometry(crs=SRID) = Field()


ewco_sensativity_raw = SourceDataset(
    name="ewco_sensativity_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    model=EwcoSensativityRaw,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/forestry_commission/woodland_creation_full_sensitivity/England_Woodland_Creation_Full_Sensitivity_Map_v4.0.shp",
)


ewco_sensativity_parcels = DerivedDataset(
    name="ewco_sensativity_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=sjoin_parcel_proportion,
    dependencies=[reference_parcels, ewco_sensativity_raw],
    model=EwcoParcels,
)

"""The Forestry Commission has developed a series of sensitivity maps, based on nationally available and consistent datasets,
to indicate where there are likely to be fewer sensitivities to woodland creation.
These maps will help to indicate to landowners whether there is likely to be potential to establish new woodland on their land,
and where there may be sensitivities that would preclude woodland creation.
"""


# EWCO datsets joined to Parcels in one big table
class EwcoMergedParcels(DataFrameModel):
    """Model for one wide table that pulls together the proportion fields for each EWCO derived dataset linked to parcels.
    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        proportion_rs: The proportion of the parcel that intersects with red squirrel sites.
        proportion_amm: The proportion of the parcel that intersects with ammonia emmissions sites.
        proportion_soc: The proportion of the parcel that intersects with nfc social sites.
        proportion_wq The proportion of the parcel that intersects with the water quality sites.
        proportion_krc: The proportion of the parcel that intersects with keeping rivers cool sites.
        proportion_fr: The proportion of the parcel that intersects with floodrisk sites.
        proportion_phn: The proportion of the parcel that intersects with the priority habitat network sites.
        proportion_sense: The proportion of the parcel that intersects with the social sustainability sites.
    """

    id_parcel: str = Field(unique=True)
    proportion_rs: float = Field(ge=0, le=1)
    proportion_amm: float = Field(ge=0, le=1)
    proportion_soc: float = Field(ge=0, le=1)
    proportion_wq: float = Field(ge=0, le=1)
    proportion_krc: float = Field(ge=0, le=1)
    proportion_fr: float = Field(ge=0, le=1)
    proportion_phn: float = Field(ge=0, le=1)
    proportion_sense: float = Field(ge=0, le=1)


ewco_parcels = DerivedDataset(
    is_geo=False,
    name="ewco_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=partial(combine_wide, sources=["rs", "amm", "soc", "wq", "krc", "fr", "phn", "sense"]),
    dependencies=[
        ewco_red_squirrel_parcels,
        ewco_ammonia_emmesions_parcels,
        ewco_nfc_social_parcels,
        ewco_waterquality_parcels,
        ewco_keeping_rivers_cool_parcels,
        ewco_flood_risk_parcels,
        ewco_priority_habitat_network_parcels,
        ewco_sensativity_parcels,
    ],
    model=EwcoMergedParcels,
)
