import shutil
from functools import reduce
from glob import iglob
from pathlib import Path

import geopandas as gpd
import pandas as pd
from geopandas.io.arrow import _geopandas_to_arrow
from pyarrow.parquet import write_to_dataset
from pyspark.serializers import AutoBatchedSerializer, PickleSerializer
from pyspark.sql import functions as F

from elmo_geo.utils.dbr import spark
from elmo_geo.utils.log import LOG
from elmo_geo.utils.misc import dbfs
from elmo_geo.utils.types import DataFrame, GeoDataFrame, PandasDataFrame, SparkDataFrame

from .convert import to_gdf


class UnknownFileExtension(Exception):
    """Don't know how to read file with extension."""


def memsize_sdf(sdf: SparkDataFrame) -> int:
    "Collect the approximate in-memory size of a SparkDataFrame."
    rdd = sdf.rdd._reserialize(AutoBatchedSerializer(PickleSerializer()))
    JavaObj = rdd.ctx._jvm.org.apache.spark.mllib.api.python.SerDe.pythonToJava(rdd._jrdd, True)
    return spark._jvm.org.apache.spark.util.SizeEstimator.estimate(JavaObj)


def auto_repartition(
    sdf: SparkDataFrame,
    count_ratio: float = 1e-6,
    mem_ratio: float = 1 / 1024**2 / 10,
    thread_ratio: float = 1.5,
    jobs_cap: int = 100_000,
    acceptance_ratio: float = 0.8,
) -> SparkDataFrame:
    """Auto repartitioning tool for SparkDataFrames.
    This uses row count, memory size, and number of JVMs to run tasks to chose the optimal partitioning.
    If the dataset is already repartitioned higher, this method doesn't coalesce those partitions.
    These default parameters have been experimentally chosen.

    Parameters:
        sdf: dataframe to repartition.
        count_ratio: with default value attempts to repartition* every 1 million rows.
        mem_ratio: * every 10 MiB.
        thread_ratio: * 1.5 tasks per thread.
        jobs_cap: limits the maximum number of jobs to fit within Spark's job limit.
        acceptance_ratio: don't repartition unless it exceeds this ratio.
    """
    partitioners = (
        round(sdf.rdd.countApprox(1000, 0.8) * count_ratio),  # 1s wait or 80% accurate.
        round(memsize_sdf(sdf) * mem_ratio),
        round(spark.sparkContext.defaultParallelism * thread_ratio),
    )
    suggested_partitions = int(min(max(partitioners), jobs_cap))
    current_partitions = sdf.rdd.getNumPartitions()
    ratio = abs(suggested_partitions - current_partitions) / current_partitions
    if acceptance_ratio < ratio:
        return sdf.repartition(suggested_partitions)
    else:
        return sdf


def load_sdf(path: str, **kwargs) -> SparkDataFrame:
    """Load SparkDataFrame from glob path.
    Automatically converts file api to spark api.
    And catches failure to coerce schemas for datasets with multiple datatypes
    (i.e. Float>Double or Timestamp_NTZ>Timestamp) that differ between partitions.
    """

    def read(f: str) -> SparkDataFrame:
        return spark.read.parquet(dbfs(f, True), **kwargs)

    def union(x: SparkDataFrame, y: SparkDataFrame) -> SparkDataFrame:
        return x.unionByName(y, allowMissingColumns=True)

    try:
        sdf = read(path)
    except Exception:  # TODO: pyspark.errors.AnalysisException, requires pyspark==3.4.1
        sdf = reduce(union, [read(f) for f in iglob(path + "*")])

    if "geometry" in sdf.columns:
        sdf = sdf.withColumn("geometry", F.expr("ST_SetSRID(ST_GeomFromWKB(geometry), 27700)"))
    return sdf


def read_file(source_path: str, is_geo: bool, layer: int | str | None = None) -> PandasDataFrame | GeoDataFrame:
    path = Path(source_path)
    if is_geo:
        if path.suffix == ".parquet" or path.is_dir():
            df = gpd.read_parquet(path)
        else:
            layers = gpd.list_layers(path)["name"]
            if layer is None and 1 < len(layers):
                df = gpd.GeoDataFrame(pd.concat((gpd.read_file(path, layer=layer, use_arrow=True).assign(layer=layer) for layer in layers), ignore_index=True))
            else:
                df = gpd.read_file(path, layer=layer, use_arrow=True)
    else:
        if path.suffix == ".parquet" or path.is_dir():
            df = pd.read_parquet(path)
        elif path.suffix == ".csv":
            df = pd.read_csv(path)
        else:
            raise UnknownFileExtension()
    return df


def write_parquet(df: DataFrame, path: str, partition_cols: list[str] | None = None):
    """Write a DataFrame to parquet and partition.
    Takes in Spark, Pandas, or GeoPandas dataframe, remove any already written data, and writes a new dataframe.

    Parameters:
        df: Dataframe to be written as (geo)parquet.
        path: Output path to write the data into.
        partition_cols: Column to write the output as separate files.
    """
    if partition_cols is None:
        partition_cols = []

    def to_gpqs(df):
        "GeoPandas writer as partial function, for applyInPandas."
        table = _geopandas_to_arrow(to_gdf(df))
        write_to_dataset(table, path, partition_cols=partition_cols)
        return pd.DataFrame([])

    def map_to_gpqs(iterator):  # DONE: I forgot mapInPandas uses an iterator.
        "Iterator of to_gpqs, for mapInPandas."
        for pdf in iterator:
            yield to_gpqs(pdf)

    path = Path(path)
    if path.exists():
        LOG.warning(f"Replacing Dataset: {path}")
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()

    path.parent.mkdir(parents=True, exist_ok=True)

    if isinstance(df, SparkDataFrame):
        if "geometry" in df.columns:
            if partition_cols:
                df.withColumn("geometry", F.expr("ST_AsBinary(geometry)")).groupby(partition_cols).applyInPandas(to_gpqs, "col struct<>").collect()
            else:
                df.withColumn("geometry", F.expr("ST_AsBinary(geometry)")).transform(auto_repartition).mapInPandas(map_to_gpqs, "col struct<>").collect()
        else:
            if partition_cols:
                df.write.parquet(dbfs(str(path), True), partitionBy=partition_cols)
            else:
                df.transform(auto_repartition).write.parquet(dbfs(str(path), True))
    elif isinstance(df, GeoDataFrame):
        to_gpqs(df)
    elif isinstance(df, PandasDataFrame):
        df.to_parquet(path, partition_cols=partition_cols)
    else:
        raise TypeError(f"Expected Spark, GeoPandas or Pandas dataframe, received {type(df)}.")
