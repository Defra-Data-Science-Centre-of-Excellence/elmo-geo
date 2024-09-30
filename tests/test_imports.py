import importlib
from glob import glob


def test_dbr():
    from elmo_geo.utils.dbr import dbutils, spark  # noqa:F401


def test_imports():
    files = glob("elmo_geo/**/*.py", recursive=True)
    for file in files:
        module = file[:-3].replace("/__init__", "").replace("/", ".")
        importlib.import_module(module)
