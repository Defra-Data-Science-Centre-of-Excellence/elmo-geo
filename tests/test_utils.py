import pytest
from elmo_geo.utils.register import register

def test_registersedona():
    try:
        register()
    except Exception:
        assert False