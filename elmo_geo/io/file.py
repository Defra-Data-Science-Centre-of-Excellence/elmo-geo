import shutil
from pathlib import Path

import geopandas as gpd
import pyarrow.parquet as pq
from geopandas.io.arrow import SUPPORTED_VERSIONS, _geopandas_to_arrow
from pyspark.sql import functions as F

from elmo_geo.utils.log import LOG
from elmo_geo.utils.misc import dbfs
from elmo_geo.utils.types import DataFrame, GeoDataFrame, PandasDataFrame, SparkDataFrame


def gdf_to_partitioned_parquet(
    gdf: gpd.GeoDataFrame,
    path: str,
    index: bool | None = None,
    geometry_encoding: str = "wkb",
    write_covering_bbox: bool = False,
    compression: str = "snappy",
    partition_cols: list[str] | None = None,
    use_deprecated_int96_timestamps: bool = True,
    **kwargs,
) -> None:
    """`geopandas.GeoDataFrame` to partitioned parquet.

    Note:
        We want to use the experimental geoparquet 1.1 here which saves as geoarrow
        instead of WKB and adds a bounding box column for predicate pushdown.
        See geopandas docs for more info, but have left the default of WKB for now
        to avoid potential incompatibility.
        https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_parquet.html#geopandas.GeoDataFrame.to_parquet
    """
    schema_version = SUPPORTED_VERSIONS[-1]
    table = _geopandas_to_arrow(gdf, index=index, schema_version=schema_version)
    pq.write_to_dataset(
        table,
        path,
        compression=compression,
        partition_cols=partition_cols,
        use_deprecated_int96_timestamps=use_deprecated_int96_timestamps,
        **kwargs,
    )


def to_pq(df: DataFrame, path: str, partition_cols: str | None = None):
    """Write any DataFrame to parquet, partition if needed.

    Parameters:
        df
        f
        partition_cols
    """
    if Path(path).exists():
        LOG.warning("Replacing Dataset")
        shutil.rmtree(path)
    if isinstance(df, SparkDataFrame):
        df.withColumn("geometry", F.expr("ST_AsBinary(geometry)")).write.parquet(dbfs(path, True), partitionBy=partition_cols, mode="overwrite")
    elif isinstance(df, GeoDataFrame):
        gdf_to_partitioned_parquet(df, path, partition_cols=partition_cols)
    elif isinstance(df, PandasDataFrame):
        df.to_parquet(path, partition_cols=partition_cols)
    else:
        raise TypeError(f"Expected Spark, GeoPandas or Pandas dataframe, received {type(df)}.")
