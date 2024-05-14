import os
import subprocess
from datetime import datetime
from glob import iglob

from fiona import listlayers
from fiona.errors import DriverError
from pyspark.sql import functions as F

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
        gdf = x
    elif isinstance(x, SparkDataFrame):
        for c in x.schema:
            if isinstance(c.dataType, SedonaType):
                x = x.withColumn(c.name, F.expr(f"ST_AsBinary({c.name})"))
        gdf = to_gdf(x.toPandas(), column, crs)
    elif isinstance(x, PandasDataFrame):
        gdf = GeoDataFrame(
            x,
            geometry=GeoSeries.from_wkb(x[column], crs=crs),
            crs=crs,
        )
    elif isinstance(x, GeoSeries):
        gdf = GeoDataFrame(geometry=x, crs=crs)
    elif isinstance(x, BaseGeometry):
        gdf = GeoDataFrame(geometry=GeoSeries(x), crs=crs)
    else:
        raise TypeError(f"Unknown type: {type(x)}")
    return gdf


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
    try:
        layers = listlayers(f)
    except DriverError:
        layers = []
    return layers


def list_files(f: str) -> list[str]:
    for f1 in iglob(f + "**", recursive=True):
        if os.path.isfile(f1):
            yield f1


def get_to_convert(f: str) -> list[tuple[str, str, str]]:
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


def ogr_to_geoparquet(f_in, f_out, layer):
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
    return out


def convert_dataset(f_in, f_out):
    for f0, part, layer in get_to_convert(f_in):
        f1 = f"{f_out}/{part}"
        ogr_to_geoparquet(f0, f1, layer)


def partition_geoparquet(f_in, f_out, columns):
    LOG.info(f"Partition: {f_in}, {f_out}")
    sdf = spark.read.parquet(dbfs(f_in, True)).withColumn("fid", F.monotonically_increasing_id())
    sdf.write.format("noop").mode("overwrite").save()  # miid bug #
    return (
        sdf.withColumnsRenamed(columns)
        .withColumn("geometry", load_geometry())
        .withColumn("geometry", F.expr("EXPLODE(ST_Dump(geometry))"))
        .transform(sindex, method="BNG", resolution="10km", index_fn="chipped_index")
        .transform(to_parquet, f_out)
    )


def convert(dataset):
    name = dataset["name"]
    columns = dataset.get("columns", {})  # rename columns, but don't drop any
    LOG.info(f"Converting: {name}")

    if hasattr(dataset, "uri"):
        dataset["bronze"] = dataset["uri"]
        dataset.pop("uri")

    f_raw = dataset["bronze"]
    f_tmp = f"/dbfs/tmp/{name}.parquet" if not f_raw.endswith(".parquet") else f_raw
    f_out = f"{SILVER}/{name}.parquet"

    # Download
    if not f_raw.startswith("/dbfs/"):
        raise TypeError(f"Expecting /dbfs/ dataset: {f_raw}")

    # Convert
    if not os.path.exists(f_tmp):
        convert_dataset(f_raw, f_tmp)

    # Partition
    if not os.path.exists(f_out):
        partition_geoparquet(f_tmp, f_out, columns)

    dataset["silver"] = f_out
    dataset["tasks"]["convert"] = datetime.today().strftime("%Y_%m_%d")
    return dataset