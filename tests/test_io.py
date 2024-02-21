import geopandas as gpd
from shapely.geometry import Point
from elmo_geo.io.convert import to_sdf
from elmo_geo.utils.register import register

def test_to_sf_geoseries():
    register()
    gs = gpd.GeoSeries([Point(0,0)])
    sdf = to_sdf(gs, column = "geometry", crs = 27700,)

def test_to_sf_basegeometry():
    register()
    g = Point(0,0)
    sdf = to_sdf(g, column = "geometry", crs = 27700,)