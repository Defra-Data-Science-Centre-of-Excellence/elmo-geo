"""`elmo_geo` datasets."""
import json
import shutil
from pathlib import Path

from elmo_geo import register
from elmo_geo.utils.log import LOG

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
    defra_grassland_proximity_parcels,
    defra_habitat_area_parcels,
    defra_heathland_proximity_parcels,
    defra_priority_habitat_england_raw,
    defra_priority_habitat_parcels,
)
from .fc_ewco import (
    ewco_nature_recovery_priority_habitat,
    ewco_nature_recovery_priority_habitat_parcels,
    ewco_nature_recovery_priority_habitat_raw,
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
    evast_habitat_mapping_raw,
    fcp_habitat_creation_type_parcel,
)
from .fr_esc_m3_trees import (
    esc_m3_geo,
    esc_m3_raw,
)
from .fr_esc_m3_trees_suitability import (
    esc_suitability_broadleaved_raw,
    esc_suitability_coniferous_raw,
    esc_suitability_riparian_raw,
    esc_tree_suitability,
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
from .rpa_reference_parcels import (
    reference_parcels,
    reference_parcels_raw,
    reference_parcels_raw_no_sbi,
)

catalogue = [
    alc_parcels,
    alc_raw,
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
    defra_grassland_proximity_parcels,
    defra_habitat_area_parcels,
    defra_heathland_proximity_parcels,
    defra_priority_habitat_england_raw,
    defra_priority_habitat_parcels,
    esc_m3_geo,
    esc_m3_raw,
    esc_suitability_broadleaved_raw,
    esc_suitability_coniferous_raw,
    esc_suitability_riparian_raw,
    esc_tree_suitability,
    evast_habitat_mapping_raw,
    ewco_nature_recovery_priority_habitat_parcels,
    ewco_nature_recovery_priority_habitat_raw,
    ewco_nature_recovery_priority_habitat,
    fcp_habitat_creation_type_parcel,
    flood_risk_areas_parcels,
    flood_risk_areas_raw,
    is_upland_parcels,
    itl2_parcels,
    itl2_raw,
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
    os_bng_parcels,
    os_bng_raw,
    os_ngd_raw,
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
    rpa_hedges_raw,  # snapshot
    sfi_agroforestry_parcels,
    sfi_agroforestry_raw,
    sfi_agroforestry,
    ward_parcels,
    ward_raw,
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
