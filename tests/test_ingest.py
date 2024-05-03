from elmo_geo.datasets.catalogue import find_datasets
from elmo_geo.io.convert import convert


def test_parcel():
    dataset_parcel = {
        "name": "rpa-parcel-adas",
        "bronze": "/dbfs/mnt/lab/restricted/ELM-Project/bronze/rpa-parcel-adas.parquet",
        "columns": {"RLR_RW_REFERENCE_PARCELS_DEC_21_LPIS_REF": "id_parcel", "Shape": "geometry"},
        "tasks": {"convert": "todo"},
    }
    return
    convert(dataset_parcel)


def test_hedge():
    dataset_hedge = {
        "name": "rpa-hedge-adas",
        "bronze": "/dbfs/mnt/lab/restricted/ELM-Project/bronze/rpa-hedge-adas.parquet",
        "columns": {"geom": "geometry"},
        "tasks": {"convert": "todo"},
    }
    return
    convert(dataset_hedge)


def test_osm():
    dataset_osm = find_datasets("osm")[0]
    return
    convert(dataset_osm)
