import pytest
from elmo_geo import register

def test_registersedona():
    try:
        register()
    except Exception:
        assert False