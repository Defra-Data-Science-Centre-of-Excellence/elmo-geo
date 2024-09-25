import geopandas as gpd
import pytest
from pyspark.sql import DataFrame as SparkDataFrame
from shapely.geometry import Point

from elmo_geo.io import read_file, to_gdf, write_parquet
from tests.test_etl import test_derived_dataset, test_source_dataset, test_source_geodataset


def _tweak_df(df):  # DONE: This is required because we now repartition on save.
    "Returns a dataframe in the original order, required due to partitioning."
    if isinstance(df, SparkDataFrame):
        df = to_gdf(df) if "geometry" in df.columns else df.toPandas()
    if {"id", "val", "class"}.issubset(df.columns):
        return (
            df.reindex(columns=["id", "val", "class"])
            .assign(**{"class": lambda df: df["class"].astype("category")})
            .sort_values(by=["class", "id"])
            .reset_index(drop=True)
        )
    elif {"id", "val"}.issubset(df.columns):
        return df.reindex(columns=["id", "val"]).sort_values(by=["id"]).reset_index(drop=True)


def _write_read_dataset(df, f, is_geo, partition_cols):
    write_parquet(df, f, partition_cols=partition_cols)
    return read_file(f, is_geo).pipe(_tweak_df)


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
    assert _tweak_df(df).equals(df_read)


@pytest.mark.dbr
def test_read_write_dataset_pdf():
    f = "/dbfs/mnt/lab/unrestricted/ELM-Project/bronze/test/test_source_dataset_io_pdf.parquet"
    df = test_source_dataset.pdf()
    df_read = _write_read_dataset(df, f, test_source_dataset.is_geo, partition_cols=None)
    assert _tweak_df(df).equals(df_read)


@pytest.mark.dbr
def test_read_write_geodataset_sdf():
    from elmo_geo.utils.register import register

    register()

    f = "/dbfs/mnt/lab/unrestricted/ELM-Project/bronze/test/test_source_geodataset_io_sdf.parquet"
    df = test_source_geodataset.sdf()
    df_read = _write_read_dataset(df, f, test_source_geodataset.is_geo, partition_cols=None)
    assert _tweak_df(df).equals(df_read)


@pytest.mark.dbr
def test_read_write_geodataset_gdf():
    f = "/dbfs/mnt/lab/unrestricted/ELM-Project/bronze/test/test_source_geodataset_io_gdf.parquet"
    df = test_source_geodataset.gdf()
    df_read = _write_read_dataset(df, f, test_source_geodataset.is_geo, partition_cols=None)
    assert _tweak_df(df).equals(df_read)


@pytest.mark.dbr
def test_read_write_dataset_partition_sdf():
    from elmo_geo.utils.register import register

    register()

    f = "/dbfs/mnt/lab/unrestricted/ELM-Project/bronze/test/test_derived_dataset_io_partitioned_sdf.parquet"
    df = test_derived_dataset.sdf()
    df_read = _write_read_dataset(df, f, test_derived_dataset.is_geo, partition_cols=["class"])
    assert _tweak_df(df).equals(df_read)


@pytest.mark.dbr
def test_read_write_dataset_partition_pdf():
    f = "/dbfs/mnt/lab/unrestricted/ELM-Project/bronze/test/test_derived_dataset_io_partitioned_pdf.parquet"
    df = test_derived_dataset.pdf()
    df_read = _write_read_dataset(df, f, test_derived_dataset.is_geo, partition_cols=["class"])
    assert _tweak_df(df).equals(df_read)
