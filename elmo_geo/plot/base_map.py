import contextily as ctx
import dotenv
import matplotlib.pyplot as plt

from elmo_geo.utils.types import GeoDataFrame

os_key = dotenv.get_key(".env", "OS_KEY")
os_provider = ctx.providers.OrdnanceSurvey.Light(key=os_key)


def plot_gdf(gdf: GeoDataFrame, figsize: tuple[float, float] = (9, 9), **kwargs):
    """GeoDataFrame plot, but with an added contextily basemap"""
    fig, ax = plt.subplots(figsize=figsize)
    ax.axis("off")
    gdf.plot(ax=ax, **kwargs)
    ctx.add_basemap(
        ax=ax,
        crs=gdf.crs or "EPSG:27700",
        source=os_provider if os_key else None,
    )
