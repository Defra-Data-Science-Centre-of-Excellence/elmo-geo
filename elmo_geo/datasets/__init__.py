"""`elmo_geo` datasets."""
import json
import os
import shutil

from elmo_geo.utils.log import LOG

from .fc_agroforestry import fc_sfi_agroforestry, fc_sfi_agroforestry_raw

catalogue = [fc_sfi_agroforestry_raw, fc_sfi_agroforestry]
"""List of datasets in `elmo_geo`."""


def write_catalogue_json():
    "Write the catalogue as a json."
    with open("data/catalogue.json", "w") as f:
        f.write(json.dumps({dataset.name: dataset.dict for dataset in catalogue}, indent=4))


def destroy_datasets():
    """Delete all datasets in the catalogue.

    Warning:
        Datasets may take long time to rebuild the next time you need them.
    """
    for dataset in catalogue:
        if os.path.exists(dataset.path):
            msg = f"Deleting {dataset.path}"
            LOG.warning(msg)
            shutil.rmtree(dataset.path)


__all__ = ["catalogue", "destroy_datasets", "write_catalogue_json"]
