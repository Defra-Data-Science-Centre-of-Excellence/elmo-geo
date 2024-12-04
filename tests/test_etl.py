import re
from datetime import datetime as dt
from glob import iglob
from importlib import import_module

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

from elmo_geo import datasets, register
from elmo_geo.datasets import catalogue
from elmo_geo.etl import Dataset, DerivedDataset, SourceDataset
from elmo_geo.etl.etl import DATE_FMT, PAT_DATE
from elmo_geo.etl.transformations import pivot_long_sdf, pivot_wide_sdf
from elmo_geo.st.udf import clean_geometries
from elmo_geo.utils.dbr import spark

test_source_dataset = SourceDataset(
    name="test_source_dataset",
    medallion="test",
    source="test",
    restricted=False,
    is_geo=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/test/test_dataset.parquet",
)
"""Test SourceDataset
"""

test_source_geodataset = SourceDataset(
    name="test_source_geodataset",
    medallion="test",
    source="test",
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
    medallion="test",
    source="test",
    restricted=False,
    is_geo=False,
    func=_make_test_dataset,
    dependencies=[],
)
"""Test DerivedDataset
"""


@pytest.mark.dbr
def test_pivots():
    sdf = spark.createDataFrame(
        pd.DataFrame(
            {
                "id": [1, 2, 3, 4],  # Unique and increasing is required
                "a": [1, 6, 3, 11],
                "b": [2, 7, 3, 12],
                "c": [3, 8, 3, 10],
                "x": [4, 9, 3, 20],
                "y": [5, 10, 3, 8],
            }
        )
    )
    sdf_long = pivot_long_sdf(sdf, ["x", "y"])
    sdf_wide = pivot_wide_sdf(sdf_long)
    pdf = sdf.toPandas()
    pdf_wide = sdf_wide.toPandas()
    pdf_wide_sorted = pdf_wide.sort_values("id", ignore_index=True)[pdf.columns]  # Spark partitions are async so this needs sorting.
    assert pdf.equals(pdf_wide_sorted)


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
    assert catalogue_diff == set(), f"The following datasets are imported but not added to the catalogue: {catalogue_diff}"

    init_diff = submodule_datasets - init_datasets
    assert init_diff == set(), f"The following datasets are created but not added to the __init__: {init_diff}"


@pytest.mark.dbr
def test_all_fresh():
    "Test all datasets are fresh."
    unfresh = [d.name for d in catalogue if not d.is_fresh]
    assert not unfresh, f"Unfresh datasets: {unfresh}"


def _dataset_date_is_most_recent(dataset):
    date = dt.strptime(dataset.date, DATE_FMT)
    pat = re.compile(PAT_DATE.format(name=dataset.name, hsh=dataset._hash))
    other_dates = [dt.strptime(pat.findall(f)[0], DATE_FMT) for f in dataset.file_matches]
    return all(date >= d for d in other_dates)


def test_all_datasets_path_most_recent():
    """For all datasets flagging as fresh in the catalogue, check that the path used is the most recent.

    Checks by using the date in the path rather than the modified time of the path because the dataset
    uses the modified time and using a different method makes the test stronger.
    """
    fails = []
    for dataset in catalogue:
        if dataset.is_fresh:
            if not _dataset_date_is_most_recent(dataset):
                fails.append(dataset)
    assert not fails, f"Not all datasets loading most recent files. Failing datasets: {[d.name for d in fails]}"


@pytest.mark.dbr
def test_source_dataset_geometry_cleaning():
    """Refreshes the test source geodataset and checks that geometries have been cleaned."""
    register()

    gdf_raw = gpd.read_file(test_source_geodataset.source_path).set_index("id").to_crs(27700)  # Source is lat,lng
    test_source_geodataset.refresh()

    gs_fresh = test_source_geodataset.gdf().set_index("id").geometry
    gs_raw_cleaned = clean_geometries(gdf_raw)

    assert gs_raw_cleaned.is_valid.all()
    assert gs_raw_cleaned.geom_equals(gs_fresh.geometry, align=True).all()
