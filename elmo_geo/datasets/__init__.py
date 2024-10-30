"""`elmo_geo` datasets."""
import json
import shutil
from pathlib import Path

from elmo_geo import register
from elmo_geo.utils.log import LOG

from .boundary import (
    boundary_adjacencies,
    boundary_hedgerows,
    boundary_relict,
    boundary_segments,
    boundary_walls,
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
from .ea_flood_risk_mapping import (
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
from .fcp_sylvan import fcp_relict_hedge_raw
from .fr_esc_m3_trees import (
    esc_carbon_parcels,
    esc_m3_geo,
    esc_m3_raw,
    esc_species_parcels,
    os_bng_no_peat_parcels,
    esc_carbon_parcels_w_50yr_totals,
)
from .fr_esc_m3_woodland_suitability import (
    esc_woodland_suitability,
    esc_woodland_suitability_rcp45_2021_2028,
)
from .hedges import (
    rpa_hedges_raw,
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
    ne_sssi_units_raw,
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

catalogue = [
    alc_parcels,
    alc_raw,
    boundary_adjacencies,
    boundary_hedgerows,
    boundary_segments,
    boundary_walls,
    boundary_relict,
    bua_parcels,
    bua_raw,
    cec_soilscapes_habitats_parcels,
    cec_soilscapes_parcels,
    cec_soilscapes_raw,
    commons_parcels,
    commons_raw,  # geopackage
    country_parcels,
    country_raw,
    cua_parcels,
    cua_raw,
    defra_habitat_area_parcels,
    defra_priority_habitat_england_raw,
    defra_priority_habitat_parcels,
    ea_rofrs_raw,
    ea_rofrs_parcels,
    ea_fz3_raw,
    ea_fz3_parcels,
    esc_m3_geo,
    esc_m3_raw,
    esc_carbon_parcels,
    esc_carbon_parcels_w_50yr_totals,
    esc_species_parcels,
    esc_woodland_suitability,
    esc_woodland_suitability_rcp45_2021_2028,
    evast_habitat_management_mapping_raw,
    evast_habitat_mapping_raw,
    ewco_nature_recovery_priority_habitat_parcels,
    ewco_nature_recovery_priority_habitat_raw,
    ewco_nature_recovery_priority_habitat,
    ewco_red_squirrel_raw,
    ewco_red_squirrel_parcels,
    ewco_nfc_social_raw,
    ewco_nfc_social_parcels,
    ewco_ammonia_emissions_raw,
    ewco_ammonia_emissions_parcels,
    ewco_flood_risk_raw,
    ewco_flood_risk_parcels,
    ewco_keeping_rivers_cool_raw,
    ewco_keeping_rivers_cool_parcels,
    ewco_parcels,
    ewco_priority_habitat_network_raw,
    ewco_priority_habitat_network_parcels,
    ewco_water_quality_raw,
    ewco_water_quality_parcels,
    ewco_sensitivity_raw,
    ewco_sensitivity_parcels,
    fcp_habitat_creation_type_parcel,
    fcp_is_phi_parcel,
    fcp_relict_hedge_raw,  # temporary, until methodology is reproduced
    flood_risk_areas_parcels,
    flood_risk_areas_raw,
    is_upland_parcels,
    itl2_parcels,
    itl2_raw,
    itl3_parcels,
    itl3_raw,
    jncc_spa_parcels,
    jncc_spa_raw,
    lad_parcels,
    lad_raw,
    living_england_habitat_map_phase_4_parcels,
    living_england_habitat_map_phase_4_raw,
    moorline_parcels,
    moorline_raw,
    national_landscapes_raw,
    national_parks_raw,
    nca_parcels,
    nca_raw,
    ne_marine_conservation_zones_parcels,
    ne_marine_conservation_zones_raw,
    ne_nnr_parcels,
    ne_nnr_raw,
    ne_ramsar_parcels,
    ne_ramsar_raw,
    ne_sac_parcels,
    ne_sac_raw,
    ne_soilscapes_habitats_raw,
    ne_sssi_units_parcels,
    ne_sssi_units_raw,  # snapshot
    os_bng_no_peat_parcels,
    os_bng_parcels,
    os_bng_raw,
    os_ngd_raw,
    osm_raw,
    osm_tidy,
    peaty_soils_parcels,
    peaty_soils_raw,
    protected_areas_parcels,
    protected_landscapes_parcels,
    protected_landscapes_tidy,
    reference_parcels_raw_no_sbi,
    reference_parcels_raw,
    reference_parcels,
    region_parcels,
    region_raw,
    rpa_land_cover_codes_raw,
    rpa_land_cover_parcels,
    rpa_land_cover_parcels_raw,
    rpa_hedges_raw,  # snapshot
    sfi_agroforestry_parcels,
    sfi_agroforestry_raw,
    sfi_agroforestry,
    ward_parcels,
    ward_raw,
    wetland_vision_parcels,
    wetland_vision_raw,
    woodland_creation_sensitivity_parcels,
    woodland_creation_sensitivity_raw,
    woodland_creation_sensitivity_var1_parcels,
    woodland_creation_sensitivity_var1_raw,
    woodland_creation_sensitivity_var1,
    woodland_creation_sensitivity_var2_parcels,
    woodland_creation_sensitivity_var2_raw,
    woodland_creation_sensitivity_var2,
    woodland_creation_sensitivity_var3_parcels,
    woodland_creation_sensitivity_var3_raw,
    woodland_creation_sensitivity_var3,
    woodland_creation_sensitivity,
]
"""List of datasets in `elmo_geo`."""


def write_catalogue_json():
    "Write the catalogue as a json."
    register()
    with open("data/catalogue.json", "w") as f:
        f.write(json.dumps({dataset.name: dataset.dict for dataset in catalogue}, indent=4))


def destroy_datasets():
    """Destroy all datasets in the catalogue.

    Warning:
        Datasets may take long time to rebuild the next time you need them.
    """
    register()
    for dataset in catalogue:
        dataset.destroy()


def main():
    """Run the whole ETL to build any missing datasets and refresh any stale ones."""
    register()
    LOG.info("Refreshing all datasets...")
    for dataset in catalogue:
        if not dataset.is_fresh:
            dataset.refresh()
    LOG.info("All datasets are up to date.")


__all__ = ["catalogue", "destroy_datasets", "write_catalogue_json"]


if __name__ == "__main__":
    main()
