from elmo_geo.datasets.catalogue import load_catalogue
from elmo_geo.datasets.datasets import datasets

catalogue = load_catalogue()
dataset_names = set(dataset.name for dataset in datasets)
catalogue_names = set(dataset["name"].split("-")[1] for dataset in catalogue)


def test_datasets_superset():
    """Test all datasets are in the catalogue.json"""
    known_differences = {
        "aonb",  # national_landscape
        "lfa",  # moorline
        "sssi",  # sssi_units
        "sssi_mask", # na
    }
    assert (catalogue_names.union(known_differences)).issuperset(dataset_names)


def test_versions_superset():
    """Test all versions are in the catalogue.json
    Not implemented, is this a requirement
    """
    assert True
