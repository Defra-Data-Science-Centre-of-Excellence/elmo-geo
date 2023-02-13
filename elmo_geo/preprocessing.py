import geopandas as gpd
from pyspark.sql.session import SparkSession
from shapely.validation import make_valid

from elmo_geo.log import LOG


def transform_crs(df: gpd.GeoDataFrame, target_epsg: int = 27700) -> gpd.GeoDataFrame:
    """If the CRS is not equal to the target CRS, then transform it.
    Parameters:
        df: A geopandas dataframe
        target_epsg: The European Petroleum Survey Group (ESPG) code for the target CRS
    Returns:
        The transformed geopandas dataframe
    """
    epsg = df.crs.to_epsg()
    if epsg != target_epsg:
        df = df.to_crs(epsg=27700)
        LOG.info(f"Converted CRS from EPSG:{epsg} to EPSG:{target_epsg}")
    return df


def tidy_columns(df: gpd.GeoDataFrame, keep_cols: list, rename_cols: dict) -> gpd.GeoDataFrame:
    """Remove unwanted columns and do some renaming
    Parameters:
        df: A geopandas dataframe
        keep_cols: The column names to keep
        rename_cols: A dictionary of column name changes
    Returns:
        The dataframe with `keep_cols` columns renamed were specified
    """
    return df.filter(keep_cols, axis="columns").rename(columns=rename_cols)


def make_geometry_valid(df: gpd.GeoDataFrame, geometry_col: str = "geometry") -> gpd.GeoDataFrame:
    """Check for invalid geometries and fix them
    Parameters:
        df: A geopandas dataframe
        geometry_col: The name of the geometry column, must be in Shapely format
    Returns:
        The dataframe with a valid geometry column
    Raises:
        AssertionError: If the geometry fix did not work
    """
    invalid = df[geometry_col].is_valid is False
    if invalid.sum() > 0:
        LOG.info(
            f"Found {invalid.sum():,.0f} invalid geometries of "
            f"{invalid.size:,.0f} ({invalid.mean():.2%})"
        )
        df[geometry_col] = df[geometry_col].map(make_valid)
        assert df[geometry_col].is_valid.all()
        LOG.info("Fixed invlid geometries")
        return df
    return df


def geometry_to_wkb(df: gpd.GeoDataFrame, geometry_col: str = "geometry") -> gpd.GeoDataFrame:
    """Convert shapely geometry column to Well Known Binary (WKB)
    Parameters:
        df: A geopandas dataframe
        geometry_col: The name of the geometry column, must be in Shapely format
    Returns:
        The dataframe with the `geometry_col` converted to WKB
    """
    df["binary"] = df[geometry_col].to_wkb()
    df = df.drop(columns=geometry_col)
    df = df.rename(columns={"binary": geometry_col})
    return df


def preprocess_dataset(
    spark: SparkSession,
    path_read: str,
    path_write: str,
    keep_cols: list,
    rename_cols: dict = {},
    target_epsg: int = 27700,
    n_partitions: int = 200,
    geometry_col: str = "geometry",
    **kwargs,
):
    """Pre-process a geospatial file for further analysis. Reads the file, explodes
        multigeometries into separate rows, transforms the CRS, cleans up the columns,
        makes the geometry valid, converts the geometry to wkb and caches to parquet.
    Parameters:
        path_read: The geospatial file to be read by geopandas
        path_write: The parquet file to be written
        target_epsg: The European Petroleum Survey Group (ESPG) code for the target CRS
        keep_cols: The column names to keep
        rename_cols: A dictionary of column name changes
        geometry_col: The name of the geometry column, must be in Shapely format
    """

    df = (
        gpd.read_file(path_read, **kwargs)
        .explode(index_parts=False)
        .pipe(transform_crs, target_epsg=target_epsg)
        .pipe(tidy_columns, keep_cols=keep_cols, rename_cols=rename_cols)
        .pipe(make_geometry_valid, geometry_col=geometry_col)
        .pipe(geometry_to_wkb, geometry_col=geometry_col)
    )
    LOG.info(f"Dataset has {df.size:,.0f} rows")
    LOG.info(f"Dataset has the following columns: {df.columns.tolist()}")
    # TODO: save to geoparquet to preserve metadata
    (
        spark.createDataFrame(df)
        .repartition(n_partitions)
        .write.format("parquet")
        .save(path_write, mode="overwrite")
    )
    LOG.info(f"Saved preprocessed dataset to {path_write}")
