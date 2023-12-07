from glob import glob
from importlib import import_module


def test_imports():
    fails = {}
    for f in glob("elmo_geo/**/*.py", recursive=True):
        m = f.replace("/", ".")[:-3]
        if f.endswith("__init__.py"):
            m = m.replace(".__init__", "")
        try:
            import_module(m)
        except Exception as e:
            fails[m] = e
    assert not fails, fails
