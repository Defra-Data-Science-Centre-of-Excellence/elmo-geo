from elmo_geo.datasets.catalogue import load_catalogue
from elmo_geo.datasets.datasets import datasets


catalogue = load_catalogue()
dataset_names = set(dataset.name for dataset in datasets)
catalogue_names = set(dataset["name"].split("-")[1] for dataset in catalogue)


def test_superset():
    known_differences = {
        "aonb",  # national_landscape
        "lfa",  # moorline
        "sssi",  # sssi_units
    }
    assert (catalogue_names.union(known_differences)).issuperset(dataset_names)
