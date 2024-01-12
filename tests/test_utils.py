import pytest
from elmo_geo import utils

def test_no_ssl_verification_ctx_basemap():
    import contextily as ctx
    import matplotlib.pyplot as plt

    try:
        fig, ax = plt.subplots(figsize=figsize)
        lims=(446450, 349550, 446650, 349850)
        ax.set(xlim=[lims[0], lims[2]], ylim=[lims[1], lims[3]])
        with utils.ssl.no_ssl_verification:
            ctx.add_basemap(
                ax=ax,
                source=ctx.providers.Thunderforest.Landscape(
                    apikey="25a2eb26caa6466ebc5c2ddd50c5dde8", attribution=None
                ),
                crs="EPSG:27700",
            )
        assert True
    except Exception:
        assert False