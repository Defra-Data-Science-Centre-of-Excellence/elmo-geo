# Databricks notebook source
# MAGIC %md
# MAGIC # Convert GeoPackage to GeoParquet
# MAGIC - RPA, EFA Hedge
# MAGIC This data is damaged.  I used ogr2ogr to fix but not convert it.  This took overnight.  The fixed copy can then be converted to geoparquet.  ogr2ogr's parquet driver is not available yet.

# COMMAND ----------

from geopandas import read_file
from pyogrio import read_info


def vector_file_to_geoparquet(f_in, f_out, *, chunksize: int = 10_000, crs: (int, str) = 27700):
    """Distributed or Parallel conversion of Vector Files to GeoParquet"""
    # mkdir(f_out)
    n = read_info(f_in)["features"]

    def convert(lower):
        """Convert Vector Files to GeoParquet"""
        upper = min(lower + chunksize, n)
        read_file(f_in, rows=slice(lower, upper)).to_crs(crs).to_parquet(
            f_out + f"/{lower}-{upper}.parquet.snappy"
        )
        return True

    return all(sc.parallelize(range(0, n, chunksize)).map(convert).collect())


# COMMAND ----------

# MAGIC %sh
# MAGIC F_IN=/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_efa_control_layer/format_GPKG_efa_control_layer/SNAPSHOT_2023_06_27_efa_control_layer/LF_CONTROL_MV.zip/LF_CONTROL_MV/LF_CONTROL_MV.gpkg
# MAGIC F_OUT=/dbfs/tmp/awest/rpa-efa_hedge-2023_06_27.gpkg
# MAGIC F_TMP=/databricks/driver/tmp.gpkg
# MAGIC
# MAGIC ogr2ogr $F_TMP $F_IN
# MAGIC mv $F_TMP $F_OUT

# COMMAND ----------

vector_file_to_geoparquet(
    # '/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_efa_control_layer/format_GPKG_efa_control_layer/SNAPSHOT_2023_06_27_efa_control_layer/LF_CONTROL_MV.zip/LF_CONTROL_MV/LF_CONTROL_MV.gpkg',
    "/dbfs/tmp/awest/rpa-efa_hedge-2023_06_27.gpkg",
    "/dbfs/tmp/awest/rpa-efa_hedge-2023_06_27.parquet",
)
