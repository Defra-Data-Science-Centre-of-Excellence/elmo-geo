"""`elmo_geo` datasets."""
from .catalog import destroy_datasets, write_catalog_json
from .fc_agroforestry import fc_sfi_agroforestry, fc_sfi_agroforestry_raw

catalog = [
    fc_sfi_agroforestry_raw,
    fc_sfi_agroforestry,
]
"""List of datasets in `elmo_geo`."""


__all__ = ["catalog", "destroy_datasets", "write_catalog_json"]
