import os
from glob import iglob
from importlib import import_module

import numpy as np
import pandas as pd
import pytest

from elmo_geo import datasets
from elmo_geo.datasets import catalogue
from elmo_geo.etl import Dataset, DerivedDataset, SourceDataset

test_source_dataset = SourceDataset(
    name="test_source_dataset",
    level0="bronze",
    level1="test",
    restricted=False,
    is_geo=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/test/test_dataset.parquet",
)
"""Test SourceDataset
"""

test_source_geodataset = SourceDataset(
    name="test_source_dataset",
    level0="bronze",
    level1="test",
    restricted=False,
    is_geo=True,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/test/test_geodataset.gpkg",
)
"""Test SourceDataset that is geographic.
"""


def _make_test_dataset():
    np.random.seed(1)
    n = 10
    return pd.DataFrame({"id": range(n), "class": ["a", "b"] * int(n / 2), "val": np.random.rand(n)})


test_derived_dataset = DerivedDataset(
    name="test_derived_dataset",
    level0="silver",
    level1="test",
    restricted=False,
    is_geo=False,
    func=_make_test_dataset,
    dependencies=[],
)
"""Test DerivedDataset
"""


@pytest.mark.dbr
def test_loads_most_recent_data():
    paths = [os.path.join(test_derived_dataset.path_dir, x) for x in os.listdir(test_derived_dataset.path_dir) if test_derived_dataset.name in x]
    dataset_gm = os.path.getmtime(test_derived_dataset.path)

    assert all(dataset_gm >= os.path.getmtime(p) for p in paths)


def test_dataset_imports():
    """Tests that datasets imported to the datasets module are also added.
    Test 1 to elmo_geo.datasets.catalogue.
    Test 2 to elmo_geo.datasets.__init__.
    """
    catalogue_datasets = {y.name for y in catalogue}
    init_datasets = {y.name for y in datasets.__dict__.values() if isinstance(y, Dataset)}
    submodule_datasets = {
        y.name
        for f in iglob("elmo_geo/datasets/*.py", recursive=True)
        if not f.endswith("__init__.py")
        for y in import_module(f[:-3].replace("/", ".")).__dict__.values()
        if isinstance(y, Dataset)
    }

    catalogue_diff = init_datasets - catalogue_datasets
    assert catalogue_diff == set(), f"The following datasets are imported but not added to the catalogue:\n{catalogue_diff}"

    init_diff = submodule_datasets - init_datasets
    assert init_diff == set(), f"The following datasets are created but not added to the __init__:\n{init_diff}"


@pytest.mark.dbr
def test_all_fresh():
    "Test all datasets are fresh."
    assert all(dataset.is_fresh for dataset in catalogue)
