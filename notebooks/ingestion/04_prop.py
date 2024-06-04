# Databricks notebook source
# MAGIC %md
# MAGIC # Overlap
# MAGIC Calculate the overlaps between features and parcels.
# MAGIC
# MAGIC Each set of columns (classes) is saved separately, with all the buffers in the save.
# MAGIC Default is no classes `"classes":{"":[]}`, and 1 buffer at zero `"buffers":[0]`.
# MAGIC
# MAGIC Example output schema,
# MAGIC `id_parcel:str,class:Any,proportion_0m:float,proportion_24m:float`,
# MAGIC where `"classes":{"class":["class"]}` and `"buffers":[0, 24]`.
# MAGIC
# MAGIC ### Method
# MAGIC 1. Rejoins dataset with parcel using the precalculated lookup.
# MAGIC 2. For each set of classes
# MAGIC     3. Buffers geometries (by 1mm) to convert them to polygons.
# MAGIC     4. Groups by id_parcel and classes, union aggregates geometry_parcel and geometry_right
# MAGIC     5. Calculates the proportion of overlap for each buffer.
# MAGIC     6. Saves dataframe and logs verifiers
# MAGIC

# COMMAND ----------

import os.path
from datetime import datetime

from pyspark.sql import functions as F

from elmo_geo import LOG, register
from elmo_geo.datasets.catalogue import find_datasets, run_task_on_catalogue
from elmo_geo.utils.misc import dbfs, info_sdf
from elmo_geo.utils.types import PandasDataFrame, SparkDataFrame

register()

# COMMAND ----------


def load_sdf(f: str) -> SparkDataFrame:
    sdf = spark.read.parquet(dbfs(f, True))
    if "geometry" in sdf.columns:
        sdf = sdf.withColumn("geometry", F.expr("ST_SetSRID(ST_GeomFromWKB(geometry), 27700)"))
    return sdf


def load_sdf_parcel_lookup(dataset: dict) -> SparkDataFrame:
    """Join a dataset with parcels using the precalculated spatial join.
    return columns: id_parcel, *classes, geometry_parcel, geometry_right
    """
    dataset_parcel = find_datasets("rpa-parcel-adas")[-1]
    classes = {col for cols in dataset.get("classes", {}).values() for col in cols}
    return (
        load_sdf(dataset["lookup_parcel"])
        .join(
            load_sdf(dataset_parcel["silver"]).withColumnRenamed("geometry", "geometry_parcel"),
            on="id_parcel",
            how="inner",
        )
        .join(
            load_sdf(dataset["silver"]).withColumnRenamed("geometry", "geometry_right"),
            on="fid",
            how="inner",
        )
    )


def calc_overlap(sdf: SparkDataFrame, classes: list[str], buffers: list[float]) -> SparkDataFrame:
    """Group by the classes, union the geometries, and calculate the overlap.
    Overlap is the proportion of overlap between geometries, with parcel as the denominator.
    All geometries are converted to polygons using a 1mm buffer (0 buffer causes TopologyError no dirEdge).
    """
    l, r = "geometry_parcel", "geometry_right"  # noqa:E741
    return (
        sdf.withColumn(l, F.expr(f"ST_Buffer({l}, 0.001)"))
        .withColumn(r, F.expr(f"ST_Buffer({r}, 0.001)"))
        .groupby("id_parcel", *classes)
        .agg(
            F.expr(f"ST_Union_Aggr({l}) AS {l}"),
            F.expr(f"ST_Union_Aggr({r}) AS {r}"),
        )
        .selectExpr(
            "id_parcel", *classes, *[f"ST_Area(ST_Intersection({l}, ST_Buffer({r}, {buffer}))) / ST_Area({l}) AS proportion_{buffer}m" for buffer in buffers]
        )
    )


def overlap_info(df: PandasDataFrame, f: str):
    """Log valid checks for the overlap task.
    Checks the percentage of proportion greater than 1, this should always be 0.  (Added a tolerance for floating point errors.)
    Also displaying the DataFrame, and describing the proportion.
    The first proportion column is selected, usually "proportion_0m".
    """
    col = [col for col in df.columns if col.startswith("proportion")][0]
    info_sdf(spark.createDataFrame(df), f, None, None)
    LOG.info(f"Proportion > 1: {(1+1e-9 < df[col]).mean():.3%}")
    LOG.info(df.sort_values(col))
    LOG.info(df[col].describe())


def overlap(dataset: dict) -> dict:
    """This task calculates a proportional overlap between a feature dataset and parcels.
    It requires lookup_parcel to be calculated first.
    Outputs: id_parcel,proportion_*m  (where * is buffers defined in the dataset).
    Saves: silver/overlap-*name-version.parquet  (where * is classes keys defined in the dataset).
    """
    f_template = dataset["lookup_parcel"].replace("/lookup_parcel-", "/overlap-{}")
    buffers, class_dict = dataset.get("buffers", [0]), dataset.get("classes", {"": []})
    sdf = load_sdf_parcel_lookup(dataset)
    for name, classes in class_dict.items():
        f = f_template.format(name)
        if not os.path.exists(f):
            LOG.info(f"Task Overlap: {dataset['name']}, {name}={classes}, buffers={buffers}, {f}")
            df = calc_overlap(sdf, classes, buffers).toPandas()
            df.to_parquet(f)
            overlap_info(df, f)
            dataset["overlap"] = dataset.get("overlap", [])
            dataset["overlap"].append(f)
    dataset["tasks"]["overlap"] = datetime.today().strftime("%Y_%m_%d")
    return dataset


# COMMAND ----------

run_task_on_catalogue("overlap", overlap)
