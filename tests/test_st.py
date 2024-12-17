import geopandas as gpd
import numpy as np
import pytest
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from elmo_geo.etl.transformations import sjoin_boundary_count, sjoin_boundary_proportion, sjoin_parcel_proportion
from elmo_geo.io.convert import to_sdf
from elmo_geo.st.segmentise import segmentise_with_tolerance
from elmo_geo.st.udf import st_udf, st_union
from elmo_geo.utils.register import register


@pytest.mark.dbr
def test_to_sdf():
    """Test conversion to SparkDataFrame is successful"""
    register()

    test = "ST_AsText(geometry) = 'LINESTRING (0 0, 1 1)'"
    g = ["LineString(0 0, 1 1)"]

    sdf = gpd.GeoDataFrame(
        geometry=gpd.GeoSeries.from_wkt(g),
    ).pipe(to_sdf)

    all_true = sdf.selectExpr(f"{test} AS test").toPandas()["test"].all()
    assert all_true == 1, all_true


@pytest.mark.dbr
def test_st_union():
    """Tests the st_union function by checking that the union of
    the line strings 'LineString(0 0, 1 1)' and
    'LineString(1 1, 2 2)' is a the multilinestring
    'MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))'.
    """
    register()

    test = "(ST_AsText(geometry) = 'MULTILINESTRING ((0 0, 1 1), (1 1, 2 2))') OR (ST_AsText(geometry) = 'MULTILINESTRING ((1 1, 2 2), (0 0, 1 1))')"
    g = ["LineString(0 0, 1 1)", "LineString(1 1, 2 2)"]

    sdf = gpd.GeoDataFrame(
        {"key": [1, 1]},
        geometry=gpd.GeoSeries.from_wkt(g),
    ).pipe(to_sdf)

    all_true = sdf.transform(st_union, ["key"]).selectExpr(f"{test} AS test").toPandas()["test"].all()
    assert all_true == 1, all_true


def prep_data(parcel_geoms: list[str], feature_geoms: list[str]) -> [SparkDataFrame, SparkDataFrame]:
    sdf_parcels = gpd.GeoDataFrame(
        {"id_parcel": [str(i) for i in range(len(parcel_geoms))]},
        geometry=gpd.GeoSeries.from_wkt(parcel_geoms),
    ).pipe(to_sdf)
    sdf_features = gpd.GeoDataFrame(
        {"class": ["a"] * len(feature_geoms)},
        geometry=gpd.GeoSeries.from_wkt(feature_geoms),
    ).pipe(to_sdf)
    return sdf_parcels, sdf_features


@pytest.mark.dbr
def test_sjoin_polygon_types():
    """Test single part parcel geometry intersecting with one single part feature."""
    register()

    parcel_geoms = ["Polygon((0 0, 0 2, 2 2, 2 0, 0 0))"]
    feature_geoms = ["LineString(0 2, 2 2)", "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"]

    df = sjoin_parcel_proportion(
        *prep_data(parcel_geoms, feature_geoms),
        columns=["class"],
    )

    prop = df.loc[0, "proportion"]
    assert prop == 0.25, prop


@pytest.mark.dbr
def test_sjoin_multipolygon_types():
    """Test multi-part parcel geometry intersecting with one single part feature."""
    register()

    parcel_geoms = ["MultiPolygon(((0 0, 0 2, 2 2, 2 0, 0 0)), ((3 3, 3 4, 4 4, 4 3, 3 3)))"]
    feature_geoms = ["LineString(0 2, 2 2)", "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"]

    df = sjoin_parcel_proportion(
        *prep_data(parcel_geoms, feature_geoms),
        columns=["class"],
    )

    prop = df.loc[0, "proportion"]
    assert prop == 0.2, prop


@pytest.mark.dbr
def test_sjoin_multipolygon_types2():
    """Test multi-part parcel geometry intersecting with two single part features."""
    register()

    parcel_geoms = ["MultiPolygon(((0 0, 0 2, 2 2, 2 0, 0 0)), ((3 3, 3 4, 4 4, 4 3, 3 3)))"]
    feature_geoms = ["LineString(0 3, 3 3)", "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))"]

    df = sjoin_parcel_proportion(
        *prep_data(parcel_geoms, feature_geoms),
        columns=["class"],
    )

    prop = df.loc[0, "proportion"]
    assert prop == 0.2, prop


@pytest.mark.dbr
def test_sjoin_multipolygon_types3():
    """Test multi-part parcel geometry intersecting with overlapping
    single part and multi-part features."""
    register()

    parcel_geoms = ["MultiPolygon(((0 0, 0 2, 2 2, 2 0, 0 0)), ((3 3, 3 4, 4 4, 4 3, 3 3)))"]
    feature_geoms = ["LineString(0 3, 3 3)", "MultiPolygon(((0 0, 0 1, 1 1, 1 0, 0 0)), ((4 4, 4 5, 5 5, 5 4, 4 4)))"]

    df = sjoin_parcel_proportion(
        *prep_data(parcel_geoms, feature_geoms),
        columns=["class"],
    )

    prop = df.loc[0, "proportion"]
    assert prop == 0.2, prop


@pytest.mark.dbr
def test_sjoin_multipolygon_types4():
    """Test multi-part parcel geometry intersecting with overlapping
    single part and multi-part features."""
    register()

    parcel_geoms = ["MultiPolygon(((0 0, 0 2, 2 2, 2 0, 0 0)), ((3 3, 3 4, 4 4, 4 3, 3 3)))"]
    feature_geoms = [
        "Polygon((1 0, 1 1, 2 1, 2 0, 1 0))",
        "Polygon((0 0, 0 1, 1 1, 1 0, 0 0))",
        "LineString(1 1, 2 2)",
        "MultiPolygon(((3 3, 3 4, 4 4, 4 3, 3 3)), ((4 4, 4 5, 5 5, 5 4, 4 4)))",
    ]

    df = sjoin_parcel_proportion(
        *prep_data(parcel_geoms, feature_geoms),
        columns=["class"],
    )

    prop = df.loc[0, "proportion"]
    assert prop == 0.6, prop


@pytest.mark.dbr
def test_sjoin_boundary_segments():
    """Test multi-part parcel geometry intersecting with overlapping
    single part and multi-part features."""
    register()

    parcel_geoms = ["Polygon((0 0, 0 21, 21 21, 21 0, 0 0))"]
    feature_geoms = ["LineString(24 0, 24 21)"]

    sdf_parcels, sdf_features = prep_data(parcel_geoms, feature_geoms)
    sdf_boundaries = (
        sdf_parcels.withColumn("geometry", F.expr("ST_Boundary(geometry)"))
        .transform(st_udf, segmentise_with_tolerance)
        .withColumn("geometry", F.expr("EXPLODE(ST_DUMP(geometry))"))
        .withColumn("id_boundary", F.monotonically_increasing_id())
    )

    df = sjoin_boundary_proportion(
        sdf_parcels,
        sdf_boundaries,
        sdf_features,
        columns=["class"],
    ).toPandas()

    observed = df.set_index("id_boundary").loc[[0, 1, 2, 3]].iloc[:, 2:].values  # drop class, id_parcel
    expected = [
        [0.0, 0.0, 0.0, 0.0, 0.0, 1.0],
        [0.0, 0.0, 0.047619, 0.238095, 0.428571, 1.0],
        [0.0, 0.0, 1.0, 1.0, 1.0, 1.0],
        [0.0, 0.0, 0.047619, 0.238095, 0.428571, 1.0],
    ]
    assert np.isclose(observed, expected, atol=1e-3).all()


@pytest.mark.dbr
def test_sjoin_boundary_count():
    """Test count of features intersecting parcel boundary segments.

    Max of three points intersecting the boundaries. Two of these are closer to one boudnary
    segment, the other is closer to a different boundary segment. This test checks that
    points are not double counted even when they overlap with multiple buffered boundary segments.
    """
    register(adaptive_partitions=False, shuffle_partitions=5, default_parallelism=5)

    parcel_geoms = ["Polygon((0 0, 0 20, 20 20, 20 0, 0 0))"]
    feature_geoms = ["Point(21 10)", "Point(25 10)", "Point(16 3)", "Point(100 100)"]

    sdf_parcels, sdf_features = prep_data(parcel_geoms, feature_geoms)
    sdf_boundaries = (
        sdf_parcels.withColumn("geometry", F.expr("ST_Boundary(geometry)"))
        .transform(st_udf, segmentise_with_tolerance)
        .withColumn("geometry", F.expr("EXPLODE(ST_DUMP(geometry))"))
        .withColumn("m", F.expr("ST_Length(geometry)"))
        .withColumn("id_boundary", F.monotonically_increasing_id())
    )

    df = sjoin_boundary_count(sdf_parcels, sdf_boundaries, sdf_features, buffers=[0, 2, 6, 10]).toPandas()

    assert "count_0m" not in df.columns

    observed = df.loc[:, ["count_2m", "count_6m", "count_10m"]].values
    expected = [[np.nan, 1, 1], [1, 2, 2]]
    assert np.array_equal(observed, expected, equal_nan=True)
