# Databricks notebook source
# MAGIC %md
# MAGIC # Spatial Join between Parcels and datasets.
# MAGIC This task creates lookup tables between rpa-parcel-adas and other datasets.
# MAGIC The outputs are saved with just id_parcel and fid for efficiency.
# MAGIC A lookup table can be joined with the original dataset, to be analysed further.

# COMMAND ----------

import os.path
from datetime import datetime

from pyspark.sql import functions as F

from elmo_geo import LOG, register
from elmo_geo.datasets.catalogue import find_datasets, run_task_on_catalogue
from elmo_geo.utils.misc import dbfs
from elmo_geo.utils.types import SparkDataFrame

register()

# COMMAND ----------


def load_sdf(f):
    return spark.read.parquet(dbfs(f, True)).withColumn("geometry", F.expr("ST_SetSRID(ST_GeomFromWKB(geometry), 27700)"))


def sjoin(
    sdf_left: SparkDataFrame,
    sdf_right: SparkDataFrame,
    how: str = "inner",
    on: str = "geometry",
    lsuffix: str = "_left",
    rsuffix: str = "_right",
    distance: float = 0,
    knn: int = 0,
) -> SparkDataFrame:
    """Spatial Join using SQL"""
    if how != "inner":
        raise NotImplementedError("sjoin: inner spatial join only")
    if on != "geometry":
        raise NotImplementedError("sjoin: geometry_column must be named geometry")
    # Add to SQL, without name conflicts
    columns_overlap = set(sdf_left.columns).intersection(sdf_right.columns)
    sdf_left.withColumnsRenamed({col: col + lsuffix for col in columns_overlap}).createOrReplaceTempView("left")
    sdf_right.withColumnsRenamed({col: col + rsuffix for col in columns_overlap}).createOrReplaceTempView("right")
    # spatial join
    if distance == 0:
        sdf = spark.sql(
            f"""
            SELECT left.*, right.*
            FROM left JOIN right
            ON ST_Intersects(left.geometry{lsuffix}, right.geometry{rsuffix})
        """
        )
    else:
        sdf = spark.sql(
            f"""
            SELECT left.*, right.*
            FROM left JOIN right
            ON ST_Distance(left.geometry{lsuffix}, right.geometry{rsuffix}) <= {distance}
        """
        )
    if 0 < knn:
        raise NotImplementedError("sjoin: nearest neighbour not supported")
    # Remove from SQL
    spark.sql("DROP TABLE left")
    spark.sql("DROP TABLE right")
    return sdf


def lookup_parcel(dataset: dict) -> dict:
    """Spaitally join .arcels with another dataset.
    Uses the same parcels as EVAST, rpa-parcel-adas
    Outputs a lookup table between
    """
    LOG.info(f"lookup_parcel: {dataset['name']}")
    dataset_parcel = find_datasets("rpa-parcel-adas")[-1]
    source, name, version = dataset["name"].split("-")
    dataset["lookup_parcel"] = dataset["silver"].replace(dataset["name"], f"lookup_parcel-{name}-{version}")
    distance, knn = dataset.get("distance", 0), dataset.get("knn", 0)
    sdf_parcel = load_sdf(dataset_parcel["silver"])
    sdf_other = load_sdf(dataset["silver"])
    if not os.path.exists(dataset["lookup_parcel"]):
        LOG.info(f"Spatially Joining: {dataset_parcel['name']} with {dataset['name']} at {distance=}m\nOutput: {dataset['lookup_parcel']}")
        sdf = (
            sjoin(
                sdf_parcel.selectExpr("id_parcel", "geometry"),
                sdf_other.select("fid", "geometry"),
                distance=distance,
                knn=knn,
            )
            .select("id_parcel", "fid")
            .drop_duplicates()
        )
        sdf.write.parquet(dbfs(dataset["lookup_parcel"], True))
    dataset["tasks"]["lookup_parcel"] = datetime.today().strftime("%Y_%m_%d")
    return dataset


# COMMAND ----------

run_task_on_catalogue("lookup_parcel", lookup_parcel)
