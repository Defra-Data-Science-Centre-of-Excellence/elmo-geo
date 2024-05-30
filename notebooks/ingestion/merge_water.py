# Databricks notebook source
# MAGIC %md
# MAGIC # Merge Water
# MAGIC Merge useful water datasets, and catagorise features as waterbody, watercourse, or ditch.
# MAGIC
# MAGIC dataset|filter|classes
# MAGIC ---|---|---
# MAGIC osm-united_kingdom | [OSM Tags](notebooks/analysis/osm_tags.py) | all
# MAGIC os-wtr_fts | `file` does not contain "catchment" | all
# MAGIC os-wtr_ntwk | | `watercourse`
# MAGIC

# COMMAND ----------

# MAGIC %sh find /dbfs/mnt/base/ -mindepth 3 -maxdepth 3 | grep "water\|river\|lake\|openstreetmap"

# COMMAND ----------


from datetime import datetime

from pyspark.sql import functions as F

from elmo_geo import register
from elmo_geo.datasets.catalogue import add_to_catalogue, find_datasets
from elmo_geo.io.file import to_parquet
from elmo_geo.st.geometry import load_geometry
from elmo_geo.utils.misc import dbfs
from elmo_geo.utils.settings import SILVER
from elmo_geo.utils.types import SparkDataFrame

register()


def load_sdf(f: str) -> SparkDataFrame:
    return spark.read.parquet(dbfs(f, True)).withColumn("geometry", F.expr("ST_SetSRID(ST_GeomFromWKB(geometry), 27700)"))


# COMMAND ----------

name = f"elmo_geo-water-{datetime.today().strftime('%Y_%m_%d')}"
dataset_water = {"name": name, "tasks": {"lookup_parcel": "todo"}, "distance": 24, "silver": f"{SILVER}/{name}.parquet"}

dataset_water

# COMMAND ----------

dataset_osm = find_datasets("osm-united_kingdom")[-1]
dataset_os_wtr_fts = find_datasets("os-wtr_fts")[-1]
dataset_os_wtr_ntwk = find_datasets("os-wtr_ntwk")[-1]

sdf_osm = load_sdf(dataset_osm["silver"]).withColumn("source", F.lit(dataset_osm["name"]))
sdf_os_wtr_fts = load_sdf(dataset_os_wtr_fts["silver"]).withColumn("source", F.lit(dataset_os_wtr_fts["name"]))
sdf_os_wtr_ntwk = load_sdf(dataset_os_wtr_ntwk["silver"]).withColumn("source", F.lit(dataset_os_wtr_ntwk["name"]))

# COMMAND ----------

sdf_osm_water = (
    sdf_osm.selectExpr(
        "source",
        """SUBSTRING(CONCAT(
            NVL(CONCAT(",highway=>", highway), ""),
            NVL(CONCAT(",waterway=>", waterway), ""),
            NVL(CONCAT(",aerialway=>", aerialway), ""),
            NVL(CONCAT(",barrier=>", barrier), ""),
            NVL(CONCAT(",man_made=>", man_made), ""),
            NVL(CONCAT(",railway=>", railway), ""),
            NVL(CONCAT(",", other_tags), "")
        ), 2) AS tags""",
        "geometry",
        "sindex",
    )
    .selectExpr(
        "source",
        """CASE
            WHEN (FALSE) THEN "ditch"
            WHEN (tags LIKE '%waterway=>%') THEN "watercourse"
            WHEN (tags LIKE '%water=>%') THEN "waterbody"
            ELSE NULL
        END AS class""",
        "geometry",
        "sindex",
    )
    .filter("class IS NOT NULL")
)


sdf_os_water = (
    sdf_os_wtr_fts.unionByName(sdf_os_wtr_ntwk, allowMissingColumns=True)
    .filter("file NOT LIKE '%catchment%'")
    .selectExpr(
        "source",
        """CASE
            WHEN (
                LOWER(description) LIKE '%drain%'
            ) THEN "ditch"
            WHEN (
                file LIKE 'wtr_ntwk_%'
                OR LOWER(description) LIKE '%watercourse%'
                OR LOWER(description) LIKE '%canal%'
            ) THEN "watercourse"
            ELSE "waterbody"
        END AS class""",
        "geometry",
        "sindex",
    )
)


sdf_water = (
    sdf_os_water.union(sdf_osm_water)
    .withColumn("geometry", F.expr("ST_Buffer(geometry, 0)"))
    .groupby("source", "class", "sindex")
    .agg(F.expr("ST_Union_Aggr(geometry)").alias("geometry"))
    .withColumn("geometry", load_geometry(encoding_fn=""))
    .withColumn("geometry", F.expr("ST_SubDivideExplode(geometry, 256)"))
    .select(F.monotonically_increasing_id().alias("fid"), "*")
    .transform(to_parquet, dataset_water["silver"])
)


add_to_catalogue([dataset_water])
display(sdf_water)
