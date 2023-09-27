import os
import shutil

import geopandas
import pandas
import pyarrow.parquet
import pyogrio
import requests
from pyspark.sql import functions as F

from elmo_geo.utils.settings import BATCHSIZE
from elmo_geo.utils.types import (
    GeoDataFrame,
    PandasDataFrame,
    SparkDataFrame,
    SparkSession,
    SedonaType,
    Union,
    GeoSeries,
    Geometry,
    BaseGeometry,
)

# Convertion between Types


def GeoDataFrame_to_PandasDataFrame(df: GeoDataFrame) -> PandasDataFrame:
    return df.to_wkb()


def PandasDataFrame_to_SparkDataFrame(
    df: PandasDataFrame, spark: SparkSession = None
) -> SparkDataFrame:
    return spark.createDataFrame(df)


def GeoDataFrame_to_SparkDataFrame(gdf: GeoDataFrame) -> SparkDataFrame:
    return gdf.pipe(GeoDataFrame_to_PandasDataFrame).pipe(PandasDataFrame_to_SparkDataFrame)


def SparkDataFrame_to_PandasDataFrame(df: SparkDataFrame) -> PandasDataFrame:
    for column in df.columns:
        if isinstance(df.schema[column].dataType, SedonaType):
            df = df.withColumn(column, F.expr(f"ST_AsBinary({column})"))
    return df.toPandas()


def PandasDataFrame_to_GeoDataFrame(
    pdf: PandasDataFrame, column: str, crs: Union[int, str]
) -> GeoDataFrame:
    return GeoDataFrame(pdf, geometry=GeoSeries.from_wkb(pdf[column], crs=crs), crs=crs).drop(
        columns=[column]
    )


def SparkDataFrame_to_GeoDataFrame(
    df: SparkDataFrame, column: str, crs: Union[int, str]
) -> GeoDataFrame:
    return SparkDataFrame_to_PandasDataFrame(df).pipe(
        PandasDataFrame_to_GeoDataFrame, column=column, crs=crs
    )


def to_gdf(
    x: Union[SparkDataFrame, Geometry], column: str = "geometry", crs: Union[int, str] = 27700
) -> GeoDataFrame:
    if isinstance(x, GeoDataFrame):
        gdf = x
    elif isinstance(x, SparkDataFrame):
        gdf = SparkDataFrame_to_GeoDataFrame(x, column=column, crs=crs)
    elif isinstance(x, PandasDataFrame):
        gdf = PandasDataFrame_to_GeoDataFrame(x, column=column, crs=crs)
    elif isinstance(x, GeoSeries):
        gdf = x.to_GeoDataFrame(crs=crs)
    elif isinstance(x, BaseGeometry):
        gdf = GeoSeries(x).to_GeoDataFrame(crs=crs)
    else:
        raise TypeError(f"Unknown type: {type(x)}")
    return gdf


def to_sdf(x: Union[SparkDataFrame, Geometry], crs: Union[int, str] = None) -> SparkDataFrame:
    if isinstance(x, SparkDataFrame):
        sdf = x
    elif isinstance(x, PandasDataFrame):
        sdf = PandasDataFrame_to_SparkDataFrame(x)
    else:
        sdf = GeoDataFrame_to_SparkDataFrame(to_gdf(x, crs))
    return sdf


# Convert format


def load_missing(column: str) -> str:
    """Returns a string that replace NULL with GeometryNULL
    Useful for non-inner joins.
    """
    null = 'ST_GeomFromText("Point EMPTY")'
    return F.expr(f"NVL({column}, {null})")
    # return F.expr(f'CASE WHEN {column} IS NULL THEN {null} ELSE {column} END')


def load_geometry(
    column: str = "geometry", from_crs: str = "EPSG:27700", encoding_fn: str = "ST_GeomFromWKB"
) -> callable:
    """Load Geometry
    Useful for ingesting data.
    Missing
      Check for NULL values and replace with an empty point
    Encoding
      encoding_fn '', 'ST_GeomFromWKB', 'ST_GeomFromWKT'
      Validate
    Standardise
      Force 2D
      Normalise ordering
      CRS = EPSG:27700 = British National Grid, which uses 1m coordinates
    Optimise
      3 = 0.001m = 1mm precision
      0 simplify to precision
    AS column
      simplify column name
    """
    null = 'ST_GeomFromText("Point EMPTY")'
    to_crs = "EPSG:27700"
    precision = 3
    string = f"ST_MakeValid({encoding_fn}({column}))"
    string = f"NVL({string}, {null})"
    # string = f'CASE WHEN {column} IS NULL THEN {null} ELSE {string} END'
    string = f'ST_Transform(ST_Normalize(ST_Force_2D({string})), "{from_crs}", "{to_crs}")'
    string = f"ST_SimplifyPreserveTopology(ST_PrecisionReduce({string}, {precision}), 0)"
    string = string + " AS " + column
    return F.expr(string)


# def manage_void(sdf:SparkDataFrame) -> SparkDataFrame:
#   '''Parquet does not support void column, so we cast to "null=Binary"
#   '''
#   null = T.BinaryType()
#   for column in sdf.columns:
#     if isinstance(sdf.schema[column].dataType, T.NullType):
#       sdf = sdf.withColumn(column, F.col(column).cast(null))
#   return sdf


# File


def dbfs(f: str, spark_api: bool):
    """Confirm the filepath to spark_api or file_api"""
    if f.startswith("dbfs:/"):
        if spark_api:
            return f
        else:
            return f.replace("dbfs:/", "/dbfs/")
    elif f.startswith("/dbfs/"):
        if spark_api:
            return f.replace("/dbfs/", "dbfs:/")
        else:
            return f
    else:
        raise Exception(f'file must start with "dbfs:/" or "/dbfs/": {f}')


def is_ingested(f_in: str, f_out: str, spark: SparkSession = None) -> SparkDataFrame:
    """Check whether or not the file has been successfully ingested already."""
    f_in, f_out = dbfs(f_in, False), dbfs(f_out, False)
    if os.path.exists(f_out):
        try:
            info = pyogrio.read_info(f_in)
            sdf = spark.read.parquet(dbfs(f_out, True))
            if info.get("features") == sdf.count():
                return True
        except Exception:
            pass
        shutil.rmtree(f_out)
    return False


def read_vector_file(f, **kwargs):
    try:
        df = geopandas.read_file(f, engine="pyogrio")
    except Exception:
        df = geopandas.read_file(f, engine="fiona")
    # Handle Timezones
    for k, v in df.dtypes.items():
        if isinstance(v, pandas.DatetimeTZDtype):
            df[k] = pandas.DatetimeIndex.to_numpy(df[k])
    return df


def ingest_part(f_in: str, f_gpq: str, lower: int, upper: int) -> bool:
    name = f"{lower:0>12}-{upper:0>12}-{{}}".format
    gdf = read_vector_file(f_in, rows=slice(lower, upper))
    table = geopandas.io.arrow._geopandas_to_arrow(gdf)
    pyarrow.parquet.write_to_dataset(table, f_gpq, partition_filename_cb=name)
    return True


def ingest_file(
    f_in: str, f_gpq: str, f_pq: str, batchsize: int = BATCHSIZE, spark: SparkSession = None
):
    f_in, f_gpq, f_pq = dbfs(f_in, False), dbfs(f_gpq, False), dbfs(f_pq, False)
    # Geospaital Info
    info = pyogrio.read_info(f_in)
    count, crs = info["features"], info["crs"]
    # Prepare GeoParquet folder
    if os.path.exists(f_gpq):
        shutil.rmtree(f_gpq)
    os.mkdir(f_gpq)
    # Partitioned GeoParquet
    for lower in range(0, count + batchsize, batchsize):
        upper = min(lower + batchsize, count)
        ingest_part(f_in, f_gpq, lower, upper)
    # Optimised Sedona Parquet
    if os.path.exists(f_pq):
        shutil.rmtree(f_pq)
    (
        spark.read.option("spark.sql.parquet.mergeSchema", True)
        .parquet(dbfs(f_gpq, True))
        .withColumn("geometry", load_geometry(from_crs=crs))
        .write.parquet(dbfs(f_pq, True), mode="overwrite")
    )
    return spark.read.parquet(dbfs(f_pq, True))


# ArcGIS


def paths_arcgis(url, batch):
    a = "/arcgis/rest/services/"
    b = "/FeatureServer/0/query?"
    f0, f1 = url.split(b)
    name = f0.split(a)[1]  # noqa:F841
    f0 += b
    f_count = f0 + "where=1%3D1&returnCountOnly=true&f=json"
    count = requests.get(f_count).json()["count"]
    paths = []
    for l in range(1, count, batch):  # noqa:E741
        u = min(l + batch, count)
        r = "objectIds=" + ",".join(str(x) for x in range(l, u)) + "&"
        path = f0 + r + f1
        paths.append(path)
    return paths


def read_arcgis(url, batch=200):
    """Slowly read dataset larger than ArcGIS's batch limit.
    ArcGIS REST API defaults to limit downloading to 200 features at a time.
    This function serially reads so exceptionally large datasets may cause OutOfMemoryError.
    Distributedly reading often causes TimeoutError.
    [Example Datasets](https://github.com/aw-west-defra/cdap_geo/blob/755b89c83ccde5cd27772a2068dfacd342661f09/cdap_geo/remotes.py#L62)  # noqa:E501
    """
    paths = paths_arcgis(url, batch)
    # for fs in paths[::BATCHSIZE // batch]:
    #   geopandas.read_file(f)
    gdf = pandas.concat(geopandas.read_file(f) for f in paths)
    return gdf


def ingest_arcgis(url):
    pass


# OSM


def read_osm(place, tags, spark=None):
    """Slowly read OSM data, then convert it to spark"""
    import osmnx

    osmnx.settings.cache_folder = "/databricks/driver/"
    osmnx.settings.timeout = 600

    sdf = (
        osmnx.geometries_from_place(place, tags)
        .reset_index()[["osmid", *tags.keys(), "geometry"]]
        .to_wkb()
        .pipe(spark.createDataFrame)
    )

    return sdf.repartition(sdf.count() // BATCHSIZE + 1).withColumn(
        "geometry", load_geometry("geometry", from_crs="EPSG:4326")
    )


def ingest_osm(f, place, tags):
    sdf = read_osm(place, tags)
    sdf.write.parquet(dbfs(f, True))
    return sdf
