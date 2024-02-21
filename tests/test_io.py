import geopandas as gpd
from shapely.geometry import Point

from elmo_geo.io.convert import to_sdf
from elmo_geo.utils.register import register


def test_to_sf_geoseries():
    register()
    to_sdf(
        gpd.GeoSeries([Point(0, 0)]),
        column="geometry",
        crs=27700,
    )


def test_to_sf_basegeometry():
    register()
    to_sdf(
        Point(0, 0),
        column="geometry",
        crs=27700,
    )
