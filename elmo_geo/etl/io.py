import shutil
import subprocess

import geopandas as gpd
import pandas as pd
import pyarrow as pa
from geopandas.io.arrow import _geopandas_to_arrow
from pyspark.sql import DataFrame as SparkDataFrame


def get_size(path: str) -> int:
    "Use GNU disk usage to find the bytes size of a tree."
    out = subprocess.run(f"du -s {path}", capture_shell=True, text=True)
    return int(out.stdout.split(" ")[0])


def clean_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    "Tidy up geometries from their original form to EPSG:27700, and precision of 1m."
    return gdf.assign(geometry=gdf.geometry.to_crs("EPSG:27700").force_2d().simplify(1).set_precision(1).remove_repeated_points(1).make_valid())


def convert_ogr(path_in: str, path_out: str) -> SparkDataFrame:
    """Read (or batch-read) geospatial dataset, and write it out as a geoparquet, using GeoPandas.
    This also converts the dataset to EPSG:27700 and reduces the geometry precision, with clean_geometry.
    """
    # Doesn't delete and replace.
    # Doesn't create subfolders.

    # Copying data locally is required for DBFS's POSIX incompatibility.
    if 10 * 1024**2 < get_size(path_in):  # <10MiB
        path_tmp = "/tmp/{}".format(path_in.split("/")[-1])
        shutil.copytree(path_in, path_tmp)
        path_in = path_tmp

    # Sequentially convert using batches.
    batchsize = 65_536  # 2^16 rows is the default parquet batchsize.
    for layer in gpd.list_layers(path_in):
        i, df = 0, [0] * batchsize
        while len(df) == batchsize:
            path_out_part = f"{path_out}/layer={layer}/part_{i}.snappy.parquet"
            gpd.read_file(path_in, layer=layer, rows=slice(i * batchsize, (i + 1) * batchsize)).pipe(clean_geometry).to_parquet(path_out_part)
            i += 1

    # Return the dataframe
    return spark.read.parquet(path_out.replace("/dbfs/", "dbfs:/"))


def pdf2gdf(pdf: pd.DataFrame) -> gpd.GeoDataFrame:
    "Convert a pd.DataFrame+WKB geometry column into gpd.GeoDataFrame, assuming EPSG:27700."
    return gpd.GeoDataFrame(
        pdf,
        geometry=gpd.GeoSeries.from_wkb(pdf["geometry"]),
        crs="EPSG:27700",
    )


def sdf2gdf(sdf: SparkDataFrame) -> gpd.GeoDataFrame:
    "Convert a SparkDataFrame+WKB geometry column into a gpd.GeoDataFrame, pdf2gdf assumes EPSG:27700."
    return sdf.toPandas().pipe(pdf2gdf)


def _get_arrow_schema(sdf: SparkDataFrame) -> pa.Schema:
    """Produce a pyarrow schema for a SparkDataFrame that contains a geometry field.
    This function combines two methods for getting a pyarrow schema. The methods need to be combined
    because going from a spark dataframe to arrow table produces the correct schema for non-geometry
    fields, while converting from a GeoDataFrame to an arrow table produces the correct geometry
    field schema.
    """
    main_table = sdf.limit(1).toArrow()
    main_schema = main_table.schema
    geo_table = _geopandas_to_arrow(sdf2gdf(sdf.limit(1)))
    geo_schema = geo_table.schema
    index = geo_schema.get_field_index("geometry")
    return main_schema.remove(index).insert(index, geo_schema.field("geometry")).with_metadata(geo_schema.metadata)


def write_geoparquet(sdf: SparkDataFrame, path: str):
    """Write a SparkDataFrame+WKB to geoparquet.
    Parameters:
        df: Dataframe to be written as (geo)parquet.
        path: Output path to write the data into.
    """
    # Doesn't delete and replace.
    # Doesn't create subfolders.
    
    schema = _get_arrow_schema(sdf)
    partitions = sdf.count() // 1_000_000  # 1_000_000 because we have cleaned geometries.

    def _fn(iterator):
        for pdf in iterator:
            table = _geopandas_to_arrow(pdf2gdf(pdf)).cast(schema)
            pa.parquet.write_to_dataset(table, path)
            return pd.DataFrame([])

    sdf.repartition(partitions).mapInPandas(_fn, "col struct<>").collect()
