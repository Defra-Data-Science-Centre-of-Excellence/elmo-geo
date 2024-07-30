"""`elmo_geo` datasets."""
import json
import shutil
from pathlib import Path

from elmo_geo import register
from elmo_geo.utils.log import LOG

from .fr_esc_m3_trees_suitability import esc_suitability_broadleaved_raw, esc_suitability_coniferous_raw, esc_suitability_riparian_raw, esc_tree_suitability
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
from .ons_itl import itl2_boundaries
from .rpa_reference_parcels import reference_parcels

catalogue = [
    esc_suitability_broadleaved_raw,
    esc_suitability_coniferous_raw,
    esc_suitability_riparian_raw,
    esc_tree_suitability,
    itl2_boundaries,
    ewco_nature_recovery_priority_habitat_parcels,
    ewco_nature_recovery_priority_habitat_raw,
    ewco_nature_recovery_priority_habitat,
    reference_parcels,
    sfi_agroforestry_raw,
    sfi_agroforestry,
    sfi_agroforestry_parcels,
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
