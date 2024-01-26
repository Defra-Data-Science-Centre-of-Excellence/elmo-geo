from pyspark.sql import functions as F

from elmo_geo.st.join import sjoin
from elmo_geo.utils.dbr import spark
from elmo_geo.utils.settings import BATCHSIZE
from elmo_geo.utils.types import SparkDataFrame, Union


def get_bng_resolution(n: int, /, target: int) -> str:
    target = n // target
    for resolution, cells in {
        "100km": 91,
        # '50km': 364,
        # '20km': 2275,
        "10km": 9100,
        # '5km': 36400,
        "1km": 910000,
    }.items():
        if target < cells:
            break
    return resolution


def get_bng_grid(resolution: str) -> SparkDataFrame:
    sf = "dbfs:/mnt/lab/restricted/ELM-Project/stg/os-bng-2023_08_24.parquet/" + resolution
    return spark.read.format("geoparquet").load(sf).withColumnRenamed('tile_name', 'sindex')


def get_grid(method: str, resolution: Union[str, int]) -> SparkDataFrame:
    if method == "BNG":
        return get_bng_grid(resolution=resolution)
    elif method == "GeoHash":
        raise NotImplementedError(method)
    elif method == "H3":
        raise NotImplementedError(method)
    elif method == "S2":
        raise NotImplementedError(method)
    else:
        methods = ["BNG", "GeoHash", "H3", "S2"]
        raise ValueError(f"{method} not in {methods}")


def multi_index(sdf: SparkDataFrame, grid: SparkDataFrame) -> SparkDataFrame:
    return sdf.transform(sjoin, grid, lsuffix="").drop("geometry_right")


def centroid_index(sdf: SparkDataFrame, grid: SparkDataFrame) -> SparkDataFrame:
    # there is the chance a centroid is on a grid line
    return (sdf
        .withColumnRenamed("geometry", "geometry_feature")
        .withColumn("geometry", F.expr("ST_Centroid(geometry_feature)"))
        .transform(sjoin, grid, lsuffix="")
        .drop("geometry", "geometry_right")
        .withColumnRenamed("geometry_feature", "geometry")
    )


def chipped_index(sdf: SparkDataFrame, grid: SparkDataFrame) -> SparkDataFrame:
    return (sdf
        .transform(sjoin, grid, lsuffix="")
        .withColumn("geometry", F.expr("ST_Intersection(geometry, geometry_right)"))
        .drop("geometry_right")
    )


def sindex(
    sdf,
    method: str = "BNG",
    resolution: str = None,
    index_join: callable = centroid_index,
):
    """Index a SparkDataFrame
    this requires geometry_column == "geometry"
    index_join: {centroid_index, chipped_index, multi_index}
    """
    if resolution is None:
        resolution = get_bng_resolution(sdf.count(), target=BATCHSIZE)
    grid = get_grid(method=method, resolution=resolution)
    return index_join(sdf, grid=grid)
