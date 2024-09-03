import geopandas as gpd
import pytest
from shapely.geometry import Point

from elmo_geo.io import read_file, write_parquet
from tests.test_etl import test_source_dataset, test_source_geodataset


def _write_read_dataset(df, p, is_geo, partition_cols):
    write_parquet(df, p, partition_cols=partition_cols)
    return read_file(p, is_geo)


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
        gdf_in = gpd.GeoDataFrame({c: [Point(0, 0)], "id": [0]})
        sdf = to_sdf(gdf_in, column=c)

        gdf_out = to_gdf(
            sdf,
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


@pytest.mark.dbr
def test_read_write_dataset_sdf():
    from elmo_geo.utils.register import register

    register()

    f = "/dbfs/mnt/lab/unrestricted/ELM-Project/bronze/test/test_source_dataset_io_sdf.parquet"
    df = test_source_dataset.sdf()
    df_read = _write_read_dataset(df, f, test_source_dataset.is_geo, partition_cols=None)
    assert df.toPandas().equals(df_read)


@pytest.mark.dbr
def test_read_write_dataset_pdf():
    f = "/dbfs/mnt/lab/unrestricted/ELM-Project/bronze/test/test_source_dataset_io_pdf.parquet"
    df = test_source_dataset.pdf()
    df_read = _write_read_dataset(df, f, test_source_dataset.is_geo, partition_cols=None)
    assert df.equals(df_read)


@pytest.mark.dbr
def test_read_write_geodataset_sdf():
    from elmo_geo.utils.register import register

    register()

    f = "/dbfs/mnt/lab/unrestricted/ELM-Project/bronze/test/test_source_geodataset_io_sdf.parquet"
    df = test_source_geodataset.sdf()
    df_read = _write_read_dataset(df, f, test_source_geodataset.is_geo, partition_cols=None)
    assert df.toPandas().equals(df_read)


@pytest.mark.dbr
def test_read_write_geodataset_gdf():
    f = "/dbfs/mnt/lab/unrestricted/ELM-Project/bronze/test/test_source_geodataset_io_gdf.parquet"
    df = test_source_geodataset.gdf()
    df_read = _write_read_dataset(df, f, test_source_geodataset.is_geo, partition_cols=None)
    assert df.toPandas().equals(df_read)
