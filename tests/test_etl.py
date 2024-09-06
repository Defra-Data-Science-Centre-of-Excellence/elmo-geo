import os

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


def test_dataset_catalogue():
    """Tests that datasets imported to the datasets module are also added to the
    elmo_geo.datasets.catalogue list.
    """
    init_datasets = [(name, cls) for name, cls in datasets.__dict__.items() if isinstance(cls, Dataset)]
    not_in_catalogue = [i for i in init_datasets if i[1] not in catalogue]
    names = "\n".join(i[0] for i in not_in_catalogue)
    assert len(not_in_catalogue) == 0, f"The following datasets are imported to elmo_geo.datasets but not added to the catalogue:\n{names}"
