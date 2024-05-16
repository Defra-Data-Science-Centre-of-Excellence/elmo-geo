from random import choice
from shutil import rmtree

import pytest

from elmo_geo.utils.register import register
from elmo_geo.datasets.catalogue import find_datasets
from elmo_geo.io.convert import convert


@pytest.mark.dbr
def test_convert():
    register()

    dataset_parcel = {
        "name": "rpa-parcel-adas",
        "columns": {"RLR_RW_REFERENCE_PARCELS_DEC_21_LPIS_REF": "id_parcel", "Shape": "geometry"},
        "tasks": {"convert": "todo"},
        "bronze": "/dbfs/mnt/lab/restricted/ELM-Project/bronze/rpa-parcel-adas.parquet",
    }
    dataset_hedge = {
        "name": "rpa-hedge-adas",
        "columns": {"geom": "geometry"},
        "tasks": {"convert": "todo"},
        "bronze": "/dbfs/mnt/lab/restricted/ELM-Project/bronze/rpa-hedge-adas.parquet",
    }
    dataset_osm = find_datasets("osm")[0]

    dataset = choice(
        [
            dataset_parcel,
            dataset_hedge,
            dataset_osm,
        ]
    )
    dataset["silver"] = "/dbfs/tmp/test.parquet"
    rmtree(dataset["silver"], ignore_errors=True)
    convert(dataset)
