import os
import subprocess
from datetime import datetime
from glob import iglob

from fiona import listlayers
from fiona.errors import DriverError
from pyspark.sql import functions as F
from pyspark.sql import types as T

from elmo_geo import LOG
from elmo_geo.io.file import to_parquet
from elmo_geo.st.geometry import load_geometry
from elmo_geo.st.index import sindex
from elmo_geo.utils.dbr import spark
from elmo_geo.utils.misc import dbfs, snake_case
from elmo_geo.utils.settings import SILVER
from elmo_geo.utils.types import (
    BaseGeometry,
    DataFrame,
    GeoDataFrame,
    Geometry,
    GeoSeries,
    PandasDataFrame,
    SedonaType,
    SparkDataFrame,
    Union,
)


def to_gdf(
    x: Union[DataFrame, Geometry],
    column: str = "geometry",
    crs: Union[int, str] = 27700,
) -> GeoDataFrame:
    """Convert anything-ish to GeoDataFrame"""
    if isinstance(x, GeoDataFrame):
        gdf = x.set_geometry(column)
    elif isinstance(x, SparkDataFrame):
        for c in x.schema:
            if isinstance(c.dataType, SedonaType):
                x = x.withColumn(c.name, F.expr(f"ST_AsBinary({c.name})"))
        gdf = to_gdf(x.toPandas(), column, crs)
    elif isinstance(x, PandasDataFrame):
        gdf = GeoDataFrame(x, geometry=GeoSeries.from_wkb(x[column]))
    elif isinstance(x, GeoSeries):
        gdf = x.to_frame(name=column)
    elif isinstance(x, BaseGeometry):
        gdf = GeoSeries(x).to_frame(name=column)
    else:
        raise TypeError(f"Unknown type: {type(x)}")
    return gdf.set_crs(crs)


def to_sdf(
    x: Union[DataFrame, Geometry],
    column: str = "geometry",
    crs: Union[int, str] = 27700,
) -> SparkDataFrame:
    """Convert anything-ish to SparkDataFrame"""
    if isinstance(x, SparkDataFrame):
        sdf = x
    elif isinstance(x, Geometry):
        # GeoDataFrames and base geometries
        sdf = to_sdf(to_gdf(x, column, crs).to_wkb(), column, crs)
    elif isinstance(x, PandasDataFrame):
        sdf = spark.createDataFrame(x).withColumn(column, F.expr(f"ST_GeomFromWKB({column})"))
    else:
        raise TypeError(f"Unknown type: {type(x)}")
    return sdf


def list_layers(f: str) -> list[str]:
    """List layers using Fiona, but don't fail, instead return an empty list"""
    try:
        layers = listlayers(f)
    except DriverError:
        layers = []
    return layers


def list_files(f: str) -> list[str]:
    """List all the files in a directory
    similar to os.walk, but yielding full paths"""
    for f1 in iglob(f + "**", recursive=True):
        if os.path.isfile(f1):
            yield f1


def get_to_convert(f: str) -> list[tuple[str, str, str]]:
    """Get all the ogr readable files and their layers in a folder"""
    if os.path.isfile(f):
        for layer in list_layers(f):
            name = f"layer={snake_case(layer)}"
            yield f, name, layer
    else:
        f = f if f.endswith("/") else f + "/"
        for f1 in list_files(f):
            for layer in list_layers(f1):
                name = f"file={snake_case(f1.replace(f, '').split('.')[0])}/layer={snake_case(layer)}"
                yield f1, name, layer


def ogr_to_geoparquet(f_in: str, f_out: str, layer: str):
    """Convert a vector file's layer into a (Geo)Parquet file using gdal>3.5 ogr2ogr"""
    os.makedirs("/".join(f_out.split("/")[:-1]), exist_ok=True)
    out = subprocess.run(
        f"""
        export CONDA_DIR=/databricks/miniconda
        export TMPDIR=/tmp
        export OGR_GEOMETRY_ACCEPT_UNCLOSED_RING=NO
        export PROJ_LIB=$CONDA_DIR/share/proj
        $CONDA_DIR/bin/ogr2ogr -t_srs EPSG:27700 -f Parquet {f_out} {f_in} {layer}
    """,
        capture_output=True,
        text=True,
        shell=True,
    )
    LOG.info(out.__repr__())


def convert_dataset(f_in: str, f_out: str):
    """Convert a folder of vector files and all their layers into a parquet dataset"""
    for f0, part, layer in get_to_convert(f_in):
        f1 = f"{f_out}/{part}"
        ogr_to_geoparquet(f0, f1, layer)


def cast_to_project_data_type(column):
    """Cast certain data types for compatibility across project operations.
    Data Types: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html
    Casting DecimalType to DoubleType because DecimalType is not considered a numeric value in Pandas.
    """
    dtype_map = {
        T.DecimalType: T.DoubleType(),
    }
    dtype = dtype_map.get(type(column.dataType), column.dataType)
    return F.col(column.name).cast(dtype).alias(column.name)


def cast_to_project_data_types(sdf: SparkDataFrame) -> SparkDataFrame:
    return sdf.select([cast_to_project_data_type(column) for column in sdf.schema])


def partition_geoparquet(f_in: str, f_out: str, columns: dict) -> SparkDataFrame:
    """Repartition vector dataset in parquet format using the chipping method at 10km"""
    LOG.info(f"Partition: {f_in}, {f_out}")
    sdf = spark.read.parquet(dbfs(f_in, True)).withColumn("fid", F.monotonically_increasing_id())
    sdf.write.format("noop").mode("overwrite").save()  # miid bug
    return (
        sdf.withColumnsRenamed(columns)
        .transform(cast_to_project_data_types)
        .withColumn("geometry", load_geometry())
        .withColumn("geometry", F.expr("EXPLODE(ST_Dump(geometry))"))
        .transform(sindex, method="BNG", resolution="10km", index_fn="chipped_index")
        .transform(to_parquet, f_out)
    )


def convert(dataset: dict) -> dict:
    """Convert a bronze vector dataset into silver
    Will recursively find files in the bronze area, and all their layers, these are columns in a SparkDataFrame.
    This method also chips the dataset into 10km grid (~3,000 paritions).
    The resulting parquet will be partitioned by file, layer, and sindex.
    `silver/source-dataset-version.parquet/file={}/layer={}/sindex={}.parquet`
    """
    name = dataset["name"]
    columns = dataset.get("columns", {})
    LOG.info(f"Converting: {name}")

    f_raw = dataset["bronze"]
    f_tmp = f"/dbfs/tmp/{name}.parquet" if (not f_raw.endswith(".parquet") and "/format_GEOPARQUET_" not in f_raw) else f_raw
    f_out = dataset.get("silver", f"{SILVER}/{name}.parquet")  # pre-existing silver is to be used for restricted data

    if not os.path.exists(f_out):
        if not os.path.exists(f_tmp):
            convert_dataset(f_raw, f_tmp)
        partition_geoparquet(f_tmp, f_out, columns)

    dataset["silver"] = f_out
    dataset["tasks"]["convert"] = datetime.today().strftime("%Y_%m_%d")
    return dataset
