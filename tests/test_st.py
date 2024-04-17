import geopandas as gpd
import pytest

from elmo_geo.io.convert import to_sdf
from elmo_geo.st.udf import st_union


@pytest.mark.dbr
def test_to_sdf():
    """Test convertion to SparkDataFrame is successful"""
    test = "ST_AsText(geometry) = 'LINESTRING (0 0, 1 1)'"
    g = ["LineString(0 0, 1 1)"]
    gdf = gpd.GeoDataFrame(geometry=gpd.GeoSeries.from_wkt(g))
    sdf = to_sdf(gdf)
    all_true = sdf.selectExpr(f"{test} AS test").toPandas()["test"].all()
    assert all_true == 1


@pytest.mark.dbr
def test_st_union():
    """Tests the st_union function by checking that the union of
    the line strings 'LineString(0 0, 1 1)' and
    'LineString(1 1, 2 2)' is a the multilinestring
    'MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))'.
    """
    test = "(ST_AsText(geometry) = 'MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))') OR (ST_AsText(geometry) = 'MULTILINESTRING ((1 1, 2 2), (0 0, 1 1))')"
    s = ["LineString(0 0, 1 1)", "LineString(1 1, 2 2)"]
    gdf = gpd.GeoDataFrame(geometry=gpd.GeoSeries.from_wkt(s))
    sdf = to_sdf(gdf)
    all_true = sdf.transform(st_union, ["key"]).selectExpr(f"{test} AS test").toPandas()["test"].all()
    assert all_true == 1
