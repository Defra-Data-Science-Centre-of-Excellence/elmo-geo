import geopandas as gpd
import pytest
from shapely.geometry import Point


@pytest.mark.dbr
def test_to_sf_geoseries():
    from elmo_geo.io.convert import to_sdf
    from elmo_geo.utils.register import register

    register()
    to_sdf(
        gpd.GeoSeries([Point(0, 0)]),
        column="geometry",
        crs=27700,
    )

@pytest.mark.dbr
def test_to_gdf_sdf():
    from elmo_geo.io.convert import to_gdf, to_sdf
    from elmo_geo.utils.register import register

    register()

    gdf_in = gpd.GeoDataFrame({"geometry_test":[Point(0,0).wkb], "id":[0]})
    sdf = to_sdf(gdf_in, column='geometry_test')

    gdf_out = to_gdf(
        gdf_in,
        column="geometry_test",
        crs=27700,
    )

    assert "geometry" in gdf_out.columns


@pytest.mark.dbr
def test_to_sf_basegeometry():
    from elmo_geo.io.convert import to_sdf
    from elmo_geo.utils.register import register

    register()
    to_sdf(
        Point(0, 0),
        column="geometry",
        crs=27700,
    )
