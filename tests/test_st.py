import geopandas as gpd
import pytest

from elmo_geo.etl.transformations import sjoin_and_proportion
from elmo_geo.io.convert import to_sdf
from elmo_geo.st.udf import st_union
from elmo_geo.utils.register import register


@pytest.mark.dbr
def test_to_sdf():
    """Test convertion to SparkDataFrame is successful"""
    register()
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
    register()
    test = "(ST_AsText(geometry) = 'MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))') OR (ST_AsText(geometry) = 'MULTILINESTRING ((1 1, 2 2), (0 0, 1 1))')"
    s = ["LineString(0 0, 1 1)", "LineString(1 1, 2 2)"]
    gdf = gpd.GeoDataFrame({"key": [1, 1]}, geometry=gpd.GeoSeries.from_wkt(s))
    sdf = to_sdf(gdf)
    all_true = sdf.transform(st_union, ["key"]).selectExpr(f"{test} AS test").toPandas()["test"].all()
    assert all_true == 1


def prep_data(parcel_geoms: list[str], feature_geoms: list[str]) -> bool:
    ids = [str(i) for i in range(len(parcel_geoms))]
    gdf = gpd.GeoDataFrame({"id_parcel": ids}, geometry=gpd.GeoSeries.from_wkt(parcel_geoms))
    sdf_parcels = to_sdf(gdf)

    gdf = gpd.GeoDataFrame({"class": ["a"] * len(feature_geoms)}, geometry=gpd.GeoSeries.from_wkt(feature_geoms))
    sdf_features = to_sdf(gdf)

    return sdf_parcels, sdf_features


@pytest.mark.dbr
def test_sjoin_polygon_types():
    """Test single part parcel geometry intersecting with one single part feature."""
    register()
    parcel_geoms = ["Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"]
    feature_geoms = ["LineString(0 1, 1 1)", "Polygon((0 0, 0 0.5, 0.5 0.5, 0.5 0, 0 0))"]
    df = sjoin_and_proportion(
        *prep_data(parcel_geoms, feature_geoms),
        columns=["class"],
    ).toPandas()
    prop = df.loc[0, "proportion"]
    assert prop == 0.25


@pytest.mark.dbr
def test_sjoin_multipolygon_types():
    """Test multi-part parcel geometry intersecting with one single part feature."""
    register()
    parcel_geoms = ["MultiPolygon(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))"]
    feature_geoms = ["LineString(0 1, 1 1)", "Polygon((0 0, 0 0.5, 0.5 0.5, 0.5 0, 0 0))"]

    df = sjoin_and_proportion(
        *prep_data(parcel_geoms, feature_geoms),
        columns=["class"],
    ).toPandas()
    prop = df.loc[0, "proportion"]
    assert prop == 0.125


@pytest.mark.dbr
def test_sjoin_multipolygon_types2():
    """Test multi-part parcel geometry intersecting with two single part features."""
    register()
    parcel_geoms = ["MultiPolygon(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))"]
    feature_geoms = ["LineString(1 1, 2 2)", "Polygon((0 0, 0 0.5, 0.5 0.5, 0.5 0, 0 0))"]

    df = sjoin_and_proportion(
        *prep_data(parcel_geoms, feature_geoms),
        columns=["class"],
    ).toPandas()
    prop = df.loc[0, "proportion"]
    assert prop == 0.125


@pytest.mark.dbr
def test_sjoin_multipolygon_types3():
    """Test multi-part parcel geometry intersecting with overlapping
    single part and multi-part features."""
    register()
    parcel_geoms = ["MultiPolygon(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))"]
    feature_geoms = ["LineString(1 1, 2 2)", "Polygon((0 0, 0 0.5, 0.5 0.5, 0.5 0, 0 0))"]

    df = sjoin_and_proportion(
        *prep_data(parcel_geoms, feature_geoms),
        columns=["class"],
    ).toPandas()
    prop = df.loc[0, "proportion"]
    assert prop == 0.125


@pytest.mark.dbr
def test_sjoin_multipolygon_types4():
    """Test multi-part parcel geometry intersecting with overlapping
    single part and multi-part features."""
    register()
    parcel_geoms = ["MultiPolygon(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))"]
    feature_geoms = [
        "Polygon((0.5 0, 0.5 0.5, 1 0.5, 1 0, 0.5 0))",
        "Polygon((0 0, 0 0.5, 1 0.5, 1 0, 0 0))",
        "LineString(1 1, 2 2)",
        "MultiPolygon(((2 2, 2 2.5, 3 2.5, 3 2, 2 2)), ((4 4, 4 5, 5 5, 5 4, 4 4)))",
    ]

    df = sjoin_and_proportion(
        *prep_data(parcel_geoms, feature_geoms),
        columns=["class"],
    ).toPandas()
    prop = df.loc[0, "proportion"]
    assert prop == 0.5
