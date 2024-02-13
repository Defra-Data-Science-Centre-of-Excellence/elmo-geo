import pytest
from elmo_geo.utils.register import register

def test_register():
    try:
        register()
    except Exception:
        assert False