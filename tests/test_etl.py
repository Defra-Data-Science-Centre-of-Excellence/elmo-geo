import os

import numpy as np
import pandas as pd

from elmo_geo.etl import DerivedDataset, SourceDataset

test_source_dataset = SourceDataset(
    name="test_source_dataset",
    level0="test",
    level1="test",
    restricted=False,
    is_geo=False,
    source_path="/dbfs/mnt/lab/unrestricted/test/test_dataset.parquet",
)
"""Test SourceDataset
"""


def _make_test_dataset():
    np.random.seed(1)
    n = 10
    return pd.DataFrame({"id": range(n), "class": ["a", "b"] * int(n / 2), "val": np.random.rand(n)})


test_derived_dataset = DerivedDataset(
    name="test_derived_dataset",
    level0="test",
    level1="test",
    restricted=False,
    is_geo=False,
    func=_make_test_dataset,
    dependencies=[],
)
"""Test DerivedDataset
"""


def test_loads_most_recent_data():
    paths = [os.path.join(test_derived_dataset.path_dir, x) for x in os.listdir(test_derived_dataset.path_dir) if test_derived_dataset.name in x]
    most_recent = sorted(paths, key=os.path.getmtime, reverse=True)[0]

    assert test_derived_dataset.path == most_recent
