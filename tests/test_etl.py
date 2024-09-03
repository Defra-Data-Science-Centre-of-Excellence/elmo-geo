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
    sdf = spark.createDataFrame({
        "id": [1, 2, 3, 4],  # Unique and increasing is required
        "a": [1, 6, 3, 11],
        "b": [2, 7, 3, 12],
        "c": [3, 8, 3, 10],
        "x": [4, 9, 3, 20],
        "y": [5, 10, 3, 8],
    })
    sdf_long = pivot_long_sdf(sdf, ["x", "y"])
    sdf_wide = pivot_wide_sdf(sdf_long)
    pdf = sdf.toPandas()
    pdf_wide = sdf_wide.toPandas()
    pdf_wide_sorted = pdf_wide.sort_values("id")[pdf.columns]  # Spark partitions are async so this needs sorting.
    assert (pdf == pdf_wide_sorted).all().all()
