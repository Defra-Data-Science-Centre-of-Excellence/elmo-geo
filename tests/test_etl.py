import os

import numpy as np
import pandas as pd
import pytest

from elmo_geo.etl import DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import pivot_long_sdf, pivot_wide_sdf

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


@pytest.mark.dbr
def test_pivots():
    sdf = spark.createDataFrame([(1, 1, 2, 3, 4, 5), (2, 6, 7, 8, 9, 10)], ["id", "a", "b", "c", "x", "y"])
    sdf_long = pivot_long_sdf(sdf, ["x", "y"])
    sdf_wide = pivot_wide_sdf(sdf_long)
    assert (sdf.toPandas() == sdf_wide.toPandas()).all().all()
