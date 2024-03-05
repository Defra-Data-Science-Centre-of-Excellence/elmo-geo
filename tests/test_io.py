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
def test_to_gdf_from_sdf():
    """Tests that the output gdf contains a geometry column.
    Also check that all input columns appear in the output.

    Runs tests for input dataframe with 'geometry' column
    and one with 'geometry_test' column.
    """
    from elmo_geo.io.convert import to_gdf, to_sdf
    from elmo_geo.utils.register import register

    register()

    for c in ["geometry", "geometry_test"]:
        gdf_in = gpd.GeoDataFrame({c:[Point(0,0).wkb], "id":[0]})
        sdf = to_sdf(gdf_in, column=c)

        gdf_out = to_gdf(
            gdf_in,
            column=c,
            crs=27700,
        )

        # Check output has a geometry column
        (gdf_out.geometry.area.to_numpy() == 0).all()

        # Check input columns contained in output
        assert set(gdf_in.columns) == set(gdf_in.columns).intersection(set(gdf_out.columns))

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
