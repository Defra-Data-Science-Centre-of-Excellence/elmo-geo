import contextily as ctx
import matplotlib.pyplot as plt
import pytest
import requests

from elmo_geo.utils.ssl import no_ssl_verification


def test_register():
    from elmo_geo.utils.register import register

    assert register()


@pytest.mark.without_cluster()
def test_good_ssl():
    """This test should pass even if no_ssl_verification fails"""
    url = "https://sha256.badssl.com/"
    with no_ssl_verification():
        requests.get(url)


@pytest.mark.without_cluster()
def test_bad_ssl():
    """This test should pass only if no_ssl_verification works"""
    url = "https://expired.badssl.com/"
    with no_ssl_verification():
        requests.get(url)


@pytest.mark.without_cluster()
def test_ctx_ssl():
    """This is a more realistic test, ensuring contextily is also available"""
    _, ax = plt.subplots()
    lims = (0, 0, 7e5, 13e5)  # OS BNG extent
    ax.set(xlim=[lims[0], lims[2]], ylim=[lims[1], lims[3]])
    with no_ssl_verification():
        ctx.add_basemap(ax=ax, crs="EPSG:27700")
