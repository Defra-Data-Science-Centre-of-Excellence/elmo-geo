"""elmo_geo datasets.
Datasets are collected into `catalogue: list[Dataset]` at the bottom.

Example use, printing all unfresh datasets.
```py
from elmo_geo.datasets import catalogue
for dataset in catalogue:
    if not dataset.is_fresh:
        print(dataset.name)
```
"""
from elmo_geo.etl import Dataset

from .boundary import (
    boundary_adjacencies,
    boundary_hedgerows,
    boundary_merger,
    boundary_merger_50p24m,
    boundary_merger_90p12m,
    boundary_relict,
    boundary_segments,
    boundary_walls,
    boundary_water_2m,
)
from .catchment_based_approach import (
    wetland_vision_parcels,
    wetland_vision_raw,
)
from .cec_soilscapes import (
    cec_soilscapes_habitats_parcels,
    cec_soilscapes_parcels,
    cec_soilscapes_raw,
    ne_soilscapes_habitats_raw,
)
from .commons import (
    commons_parcels,
    commons_raw,
)
from .defra_alc import (
    alc_parcels,
    alc_raw,
)
from .defra_flood_risk_areas import (
    flood_risk_areas_parcels,
    flood_risk_areas_raw,
)
from .defra_national_character_areas import (
    nca_parcels,
    nca_raw,
)
from .defra_priority_habitats import (
    defra_habitat_area_parcels,
    defra_priority_habitat_england_raw,
    defra_priority_habitat_parcels,
)
from .ea_alert import (
    ea_olf_parcels,
    ea_olf_raw,
)
from .ea_flood_risk_mapping import (
    ea_fz2_parcels,
    ea_fz2_raw,
    ea_fz3_parcels,
    ea_fz3_raw,
    ea_rofrs_parcels,
    ea_rofrs_raw,
)
from .fc_ewco import (
    ewco_ammonia_emissions_parcels,
    ewco_ammonia_emissions_raw,
    ewco_flood_risk_parcels,
    ewco_flood_risk_raw,
    ewco_keeping_rivers_cool_parcels,
    ewco_keeping_rivers_cool_raw,
    ewco_nature_recovery_priority_habitat,
    ewco_nature_recovery_priority_habitat_parcels,
    ewco_nature_recovery_priority_habitat_raw,
    ewco_nfc_social_parcels,
    ewco_nfc_social_raw,
    ewco_parcels,
    ewco_priority_habitat_network_parcels,
    ewco_priority_habitat_network_raw,
    ewco_red_squirrel_parcels,
    ewco_red_squirrel_raw,
    ewco_sensitivity_parcels,
    ewco_sensitivity_raw,
    ewco_water_quality_parcels,
    ewco_water_quality_raw,
)
from .fc_woodland_sensitivity import (
    sfi_agroforestry,
    sfi_agroforestry_parcels,
    sfi_agroforestry_raw,
    woodland_creation_sensitivity,
    woodland_creation_sensitivity_parcels,
    woodland_creation_sensitivity_raw,
    woodland_creation_sensitivity_var1,
    woodland_creation_sensitivity_var1_parcels,
    woodland_creation_sensitivity_var1_raw,
    woodland_creation_sensitivity_var2,
    woodland_creation_sensitivity_var2_parcels,
    woodland_creation_sensitivity_var2_raw,
    woodland_creation_sensitivity_var3,
    woodland_creation_sensitivity_var3_parcels,
    woodland_creation_sensitivity_var3_raw,
)
from .fcp_habitat_classification import (
    evast_habitat_management_mapping_raw,
    evast_habitat_mapping_raw,
    fcp_habitat_creation_type_parcel,
    fcp_is_phi_parcel,
)
from .fcp_parcels_geojson import (
    reference_parcels_bng_geojson,
)
from .fcp_sylvan import (
    fcp_relict_hedge_raw,  # TODO: temporary
)
from .fcp_tree_detection import (
    fcp_boundary_tree_count,
    fcp_interior_tree_count,
    fcp_tree_detection_raw,  # TODO: temporary
)
from .fr_esc_m3_trees import (
    esc_carbon_parcels,
    esc_carbon_parcels_w_50yr_total,
    esc_m3_geo,
    esc_m3_raw,
    esc_species_parcels,
    os_bng_no_peat_parcels,
)
from .fr_esc_m3_woodland_suitability import (
    esc_woodland_suitability,
    esc_woodland_suitability_rcp45_2021_2028,
)
from .hedges import (
    rpa_hedges_raw,  # TODO: snapshot
)
from .historic import (
    he_lb_raw,
    he_pws_raw,
    he_rb_raw,
    he_rpg_raw,
    he_shine_raw,
    he_sm_raw,
    he_whs_raw,
)
from .living_england import (
    living_england_habitat_map_phase_4_parcels,
    living_england_habitat_map_phase_4_raw,
)
from .moor import (
    is_upland_parcels,
    moorline_parcels,
    moorline_raw,
)
from .ons import (
    bua_parcels,
    bua_raw,
    country_parcels,
    country_raw,
    cua_parcels,
    cua_raw,
    itl2_parcels,
    itl2_raw,
    itl3_parcels,
    itl3_raw,
    lad_parcels,
    lad_raw,
    region_parcels,
    region_raw,
    ward_parcels,
    ward_raw,
)
from .os import (
    os_bng_parcels,
    os_bng_raw,
    os_ngd_raw,
)
from .osm import (
    osm_raw,
    osm_tidy,
)
from .peat import (
    peaty_soils_parcels,
    peaty_soils_raw,
)
from .protected_areas import (
    jncc_spa_parcels,
    jncc_spa_raw,
    ne_marine_conservation_zones_parcels,
    ne_marine_conservation_zones_raw,
    ne_nnr_parcels,
    ne_nnr_raw,
    ne_ramsar_parcels,
    ne_ramsar_raw,
    ne_sac_parcels,
    ne_sac_raw,
    ne_sssi_units_parcels,
    ne_sssi_units_raw,  # snapshot
    protected_areas_parcels,
)
from .protected_landscapes import (
    national_landscapes_raw,
    national_parks_raw,
    protected_landscapes_parcels,
    protected_landscapes_tidy,
)
from .rpa_land_cover import (
    rpa_land_cover_codes_raw,
    rpa_land_cover_parcels,
    rpa_land_cover_parcels_raw,
)
from .rpa_reference_parcels import (
    reference_parcels,
    reference_parcels_raw,
    reference_parcels_raw_no_sbi,
)
from .ukceh_fertilisers import (
    ukceh_fertilisers_parcels,
    ukceh_nitrogen_parcels,
    ukceh_nitrogen_raw,
    ukceh_phosphorus_parcels,
    ukceh_phosphorus_raw,
    ukceh_potassium_parcels,
    ukceh_potassium_raw,
)
from .wfm import (
    wfm_farms,
    wfm_info,
    wfm_parcels,
)

catalogue = list(v for v in locals().values() if isinstance(v, Dataset))
