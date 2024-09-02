import shutil
from pathlib import Path

import geopandas as gpd
import pandas as pd
from geopandas.io.arrow import _geopandas_to_arrow
from pyarrow.parquet import write_to_dataset
from pyspark.sql import functions as F

from elmo_geo.utils.log import LOG
from elmo_geo.utils.misc import dbfs
from elmo_geo.utils.types import DataFrame, GeoDataFrame, PandasDataFrame, SparkDataFrame

from .convert import to_gdf


class UnknownFileExtension(Exception):
    """Don't know how to read file with extension."""


def read_file(source_path: str, is_geo: bool) -> PandasDataFrame | GeoDataFrame:
    path = Path(source_path)
    if is_geo:
        if path.suffix == ".parquet" or path.is_dir():
            df = gpd.read_parquet(path)
        else:
            layers = gpd.list_layers(path)["name"]
            if 1 < len(layers):
                df = gpd.GeoDataFrame(pd.concat((gpd.read_file(path, layer=layer).assign(layer=layer) for layer in layers), ignore_index=True))
            else:
                df = gpd.read_file(path)
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
        "GeoPandas writer as partial function."
        table = _geopandas_to_arrow(to_gdf(df))
        write_to_dataset(table, path, partition_cols=partition_cols)
        return pd.DataFrame([])

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
            df.withColumn("geometry", F.expr("ST_AsBinary(geometry)")).groupby(partition_cols).applyInPandas(to_gpqs, "col struct<>").collect()
        else:
            df.write.parquet(dbfs(str(path), True), partitionBy=partition_cols)
    elif isinstance(df, GeoDataFrame):
        to_gpqs(df)
    elif isinstance(df, PandasDataFrame):
        df.to_parquet(path, partition_cols=partition_cols)
    else:
        raise TypeError(f"Expected Spark, GeoPandas or Pandas dataframe, received {type(df)}.")
