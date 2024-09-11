"""`elmo_geo` datasets."""
import json
import shutil
from pathlib import Path

from elmo_geo import register
from elmo_geo.utils.log import LOG

from .defra_priority_habitats import (
    defra_grassland_proximity_parcels,
    defra_heathland_proximity_parcels,
    defra_priority_habitat_england,
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
from .fr_esc_m3_trees import (
    esc_native_broadleaved_26_raw,
    esc_native_broadleaved_45_raw,
    esc_native_broadleaved_60_raw,
    esc_native_broadleaved_85_raw,
    esc_productive_conifer_26_raw,
    esc_productive_conifer_45_raw,
    esc_productive_conifer_60_raw,
    esc_productive_conifer_85_raw,
    esc_riparian_26_raw,
    esc_riparian_45_raw,
    esc_riparian_60_raw,
    esc_riparian_85_raw,
    esc_silvoarable_26_raw,
    esc_silvoarable_45_raw,
    esc_silvoarable_60_raw,
    esc_silvoarable_85_raw,
    esc_wood_pasture_26_raw,
    esc_wood_pasture_45_raw,
    esc_wood_pasture_60_raw,
    esc_wood_pasture_85_raw,
)
from .fr_esc_m3_trees_suitability import esc_suitability_broadleaved_raw, esc_suitability_coniferous_raw, esc_suitability_riparian_raw, esc_tree_suitability
from .ons_itl import itl2_boundaries_parcels, itl2_boundaries_raw
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
from .rpa_reference_parcels import reference_parcels, reference_parcels_raw, reference_parcels_raw_no_sbi

catalogue = [
    protected_areas_parcels,
    ne_sssi_units_raw,
    ne_sssi_units_parcels,
    ne_nnr_raw,
    ne_nnr_parcels,
    ne_sac_raw,
    ne_sac_parcels,
    jncc_spa_raw,
    jncc_spa_parcels,
    ne_ramsar_raw,
    ne_ramsar_parcels,
    ne_marine_conservation_zones_raw,
    ne_marine_conservation_zones_parcels,
    esc_native_broadleaved_26_raw,
    esc_native_broadleaved_45_raw,
    esc_native_broadleaved_60_raw,
    esc_native_broadleaved_85_raw,
    esc_productive_conifer_26_raw,
    esc_productive_conifer_45_raw,
    esc_productive_conifer_60_raw,
    esc_productive_conifer_85_raw,
    esc_riparian_26_raw,
    esc_riparian_45_raw,
    esc_riparian_60_raw,
    esc_riparian_85_raw,
    esc_silvoarable_26_raw,
    esc_silvoarable_45_raw,
    esc_silvoarable_60_raw,
    esc_silvoarable_85_raw,
    esc_wood_pasture_26_raw,
    esc_wood_pasture_45_raw,
    esc_wood_pasture_60_raw,
    esc_wood_pasture_85_raw,
    esc_suitability_broadleaved_raw,
    esc_suitability_coniferous_raw,
    esc_suitability_riparian_raw,
    esc_tree_suitability,
    defra_priority_habitat_england,
    defra_priority_habitat_parcels,
    defra_heathland_proximity_parcels,
    defra_grassland_proximity_parcels,
    ewco_nature_recovery_priority_habitat_parcels,
    ewco_nature_recovery_priority_habitat_raw,
    ewco_nature_recovery_priority_habitat,
    ewco_nature_recovery_priority_habitat_parcels,
    itl2_boundaries_raw,
    itl2_boundaries_parcels,
    reference_parcels_raw,
    reference_parcels_raw_no_sbi,
    reference_parcels,
    sfi_agroforestry_raw,
    sfi_agroforestry,
    sfi_agroforestry_parcels,
    woodland_creation_sensitivity_raw,
    woodland_creation_sensitivity,
    woodland_creation_sensitivity_parcels,
    woodland_creation_sensitivity_var1_raw,
    woodland_creation_sensitivity_var1,
    woodland_creation_sensitivity_var1_parcels,
    woodland_creation_sensitivity_var2_raw,
    woodland_creation_sensitivity_var2,
    woodland_creation_sensitivity_var2_parcels,
    woodland_creation_sensitivity_var3_raw,
    woodland_creation_sensitivity_var3,
    woodland_creation_sensitivity_var3_parcels,
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
