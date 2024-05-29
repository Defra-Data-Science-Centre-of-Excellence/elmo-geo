# Databricks notebook source
# MAGIC %md
# MAGIC # Overlap
# MAGIC Calculate the overlaps between features and parcels.
# MAGIC [OSM Tags](notebooks/analysis/osm_tags)
# MAGIC ### TODO
# MAGIC - copy output to s3
# MAGIC - document
# MAGIC - analysis
# MAGIC     ```py
# MAGIC     proportion_over_1 = (result.toPandas().proportion > 1.0).sum()
# MAGIC     if proportion_over_1:
# MAGIC         LOG.info(f"{proportion_over_1:,.0f} parcels have a feature overlapping by a proportion > 1 ({proportion_over_1/count:%})")
# MAGIC     result.toPandas().proportion.describe()
# MAGIC     ```

# COMMAND ----------

import os.path
from pyspark.sql import functions as F

from elmo_geo import register, LOG
from elmo_geo.datasets.catalogue import find_datasets, run_task_on_catalogue
from elmo_geo.utils.misc import dbfs, info_sdf
from elmo_geo.utils.types import SparkDataFrame
from elmo_geo.st.udf import st_union

register()

# COMMAND ----------

def load_sdf(f):
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
    return (load_sdf(dataset["lookup_parcel"])
        .join(
            (
                load_sdf(dataset_parcel["silver"])
                .transform(st_union, "id_parcel")
                .withColumnRenamed("geometry", "geometry_parcel")
            ),
            on = "id_parcel",
            how = "inner",
        )
        .join(
            (
                load_sdf(dataset["silver"])
                .transform(st_union, ["fid", *classes])
                .withColumnRenamed("geometry", "geometry_right")
            ),
            on = "fid",
            how = "inner",
        )
    )

def calc_overlap(sdf: SparkDataFrame, classes: list[str], buffers: list[float]) -> SparkDataFrame:
    """todo
    """
    l, r = "geometry_parcel", "geometry_right"
    return (sdf
        .groupby("id_parcel", *classes)
        .agg(
            F.expr(f"ST_Union_Aggr(ST_Buffer({l}, 0)) AS {l}"),
            F.expr(f"ST_Union_Aggr(ST_Buffer({r}, 0)) AS {r}"),
        )
        .selectExpr(
            "id_parcel", *classes, *[
                f"ST_Area(ST_Intersection({l}, ST_Buffer({r}, {buffer}))) / ST_Area({l}) AS proportion_{buffer}m"
                for buffer in buffers
            ]
        )
    )

def head_and_tail(sdf: SparkDataFrame, skip: int, n: int) -> SparkDataFrame:
    """todo
    """
    return sdf.sort(sdf.columns[skip]).transform(lambda df: df.head(n).union(df.tail(n))).toPandas()

def overlap(dataset: dict) -> dict:
    """todo
    """
    f_template = dataset["lookup_parcel"].replace("/lookup_parcel-", "/overlap-{}")
    buffers, class_dict = dataset.get("buffers", [0]), dataset.get("classes", {"":[]})
    sdf = load_sdf_parcel_lookup(dataset)
    for name, classes in class_dict.items():
        f = f_template.format(name)
        if not os.path.exists(f):
            LOG.info(f"Task Overlap: {dataset['name']}, {name}={classes}, buffers={buffers}, {f}")
            sdf = calc_overlap(sdf, classes, buffers)
            sdf.write.parquet(dbfs(f, True))
            info_sdf(sdf, f, None, None)
            LOG.info(head_and_tail(sdf, len(classes)+1, 5))
            dataset["overlap"] = dataset.get("overlap", [])
            dataset["overlap"].append(f)
    return dataset


# COMMAND ----------

run_task_on_catalogue("overlap", overlap)
