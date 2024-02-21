import importlib
from glob import glob

# import pytest


# @pytest.mark.without_cluster
def test_dbr():
    from elmo_geo.utils.dbr import dbutils, spark  # noqa:F401


# @pytest.mark.without_cluster
def test_imports():
    files = glob("elmo_geo/**/*.py", recursive=True)
    for file in files:
        module = file[:-3].replace("/__init__", "").replace("/", ".")
        assert importlib.import_module(module)
