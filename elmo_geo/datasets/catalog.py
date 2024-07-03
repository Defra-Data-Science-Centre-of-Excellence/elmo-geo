"""Catalog of available datasets and helper functions."""
import json
import os
import shutil

from elmo_geo.utils.log import LOG

from .fc_agroforestry import fc_sfi_agroforestry, fc_sfi_agroforestry_raw

catalog = [fc_sfi_agroforestry_raw, fc_sfi_agroforestry]
"""List of datasets in `elmo_geo`."""


def write_catalog_json():
    "Write the catalog as a json."
    with open("data/catalog.json", "w") as f:
        f.write(json.dumps({dataset.name: dataset.dict for dataset in catalog}, indent=4))


def destroy_datasets():
    """Delete all datasets in the catalog.

    Warning:
        Datasets may take long time to rebuild the next time you need them.
    """
    for dataset in catalog:
        if os.path.exists(dataset.path):
            msg = f"Deleting {dataset.path}"
            LOG.warning(msg)
            shutil.rmtree(dataset.path)
