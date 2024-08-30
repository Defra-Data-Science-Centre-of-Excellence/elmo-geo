import shutil
from pathlib import Path

from geopandas.io.arrow import _geopandas_to_arrow
from pyarrow.dataset import write_dataset
from pyspark.sql import functions as F

from elmo_geo.utils.log import LOG
from elmo_geo.utils.types import DataFrame, GeoDataFrame, PandasDataFrame, SparkDataFrame


def to_parquet(df: DataFrame, path: str, partition_cols: str | None = None):
    """Write a DataFrame to parquet and partition.
    Takes in Spark, Pandas, or GeoPandas dataframe, remove any already written data, and writes a new dataframe.

    Parameters:
        df: Dataframe to be written as (geo)parquet.
        path: Output path to write the data into.
        partition_cols: Column to write the output as separate files.
    """

    def to_gpqs(df):
        "geopandas writer as partial function"
        table = _geopandas_to_arrow(df)
        write_dataset(table, path, format="parquet", partition_cols=partition_cols)

    if Path(path).exists():
        LOG.warning("Replacing Dataset")
        shutil.rmtree(path)
    if isinstance(df, SparkDataFrame):
        df.withColumn("geometry", F.expr("ST_AsBinary(geometry)")).groupby(partition_cols).applyInPandas(to_gpqs, "")
    elif isinstance(df, GeoDataFrame):
        to_gpqs(df)
    elif isinstance(df, PandasDataFrame):
        df.to_parquet(path, partition_cols=partition_cols)
    else:
        raise TypeError(f"Expected Spark, GeoPandas or Pandas dataframe, received {type(df)}.")
