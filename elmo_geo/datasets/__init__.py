"""`elmo_geo` datasets."""
import json
import shutil
from pathlib import Path

from elmo_geo.utils.log import LOG

from .esc_m3_trees_suitability import esc_suitability_broadleaved_raw, esc_suitability_coniferous_raw, esc_suitability_riparian_raw, esc_tree_suitability
from .fc_agroforestry import sfi_agroforestry, sfi_agroforestry_parcels, sfi_agroforestry_raw
from .rpa_reference_parcels import reference_parcels

catalogue = [
    reference_parcels,
    sfi_agroforestry_raw,
    sfi_agroforestry,
    sfi_agroforestry_parcels,
    esc_suitability_broadleaved_raw,
    esc_suitability_coniferous_raw,
    esc_suitability_riparian_raw,
    esc_tree_suitability,
]
"""List of datasets in `elmo_geo`."""


def write_catalogue_json():
    "Write the catalogue as a json."
    with open("data/catalogue.json", "w") as f:
        f.write(json.dumps({dataset.name: dataset.dict for dataset in catalogue}, indent=4))


def destroy_datasets():
    """Destroy all datasets in the catalogue.

    Warning:
        Datasets may take long time to rebuild the next time you need them.
    """
    for dataset in catalogue:
        dataset.destroy()


def main():
    """Run the whole ETL to build any missing datasets and refresh any stale ones."""
    LOG.info("Refreshing all datasets...")
    for dataset in catalogue:
        if not dataset.is_fresh:
            dataset.refresh()
    LOG.info("All datasets are up to date.")


__all__ = ["catalogue", "destroy_datasets", "write_catalogue_json"]


if __name__ == "__main__":
    main()
