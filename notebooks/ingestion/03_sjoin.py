# Databricks notebook source
# MAGIC %md
# MAGIC # Spatial Join between Parcels and datasets.
# MAGIC
# MAGIC ### notes
# MAGIC - [ ] 02_splitting_method snapping to a segmented boundary
# MAGIC - [ ] 03_metrics should contain the output table
# MAGIC - [ ] move Business Info to another place
# MAGIC - [ ] load_sdf
# MAGIC - [ ] delete copied bits from aw-notebooks once merged
# MAGIC - QA
# MAGIC   - [x] check the overlaps of rpa-parcel-adas
# MAGIC   - [ ] ensure buffer totals are less than parcel total area
# MAGIC   - [ ] ensure meter total is reasonable
# MAGIC   - [ ] boundary segmentation method
# MAGIC   - [ ] review my OSM and OS-NGD filters
# MAGIC
# MAGIC ### todo
# MAGIC - [ ] Review: 02_historic
# MAGIC - [ ] Merge: water
# MAGIC - [ ] sjoin proportion
# MAGIC

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
    how: str = 'outer', on: str = 'geometry',
    lsuffix: str = '_left', rsuffix: str = '_right',
    distance: float = 0, knn: int = 0,
) -> SparkDataFrame:
    """Spatial Join using SQL
    """
    if how != "outer":
        raise NotImplementedError("sjoin: outer spatial join only")
    if on != "geometry":
        raise NotImplementedError("sjoin: geometry_column must be named geometry")
    # Add to SQL, without name conflicts
    columns_overlap = set(sdf_left.columns).intersection(sdf_right.columns)
    sdf_left.withColumnsRenamed({col: col+lsuffix for col in columns_overlap}).createOrReplaceTempView("left")
    sdf_right.withColumnsRenamed({col: col+rsuffix for col in columns_overlap}).createOrReplaceTempView("right")
    # spatial join
    if distance == 0:
        sdf = spark.sql(f"""
            SELECT left.*, right.*
            FROM left FULL OUTER JOIN right
            ON ST_Intersects(left.geometry{lsuffix}, right.geometry{rsuffix})
        """)
    else:
        sdf = spark.sql(f"""
            SELECT left.*, right.*
            FROM left FULL OUTER JOIN right
            ON ST_Distance(left.geometry{lsuffix}, right.geometry{rsuffix}) <= {distance}
        """)
    if 0 < knn:
        raise NotImplementedError("sjoin: nearest neighbour not supported")
    # Remove from SQL
    spark.sql("DROP TABLE left")
    spark.sql("DROP TABLE right")
    return sdf

def lookup_parcel(dataset: dict) -> dict:
    """Spaitally Join Parcels with another dataset
    Uses the same parcels as EVAST, rpa-parcel-adas
    Outputs a lookup table between
    """
    dataset_parcel = find_datasets("rpa-parcel-adas")[-1]
    source, name, version = dataset["name"].split('-')
    dataset["lookup_parcel"] = dataset["silver"].replace(dataset["name"], f"lookup_parcel-{name}-{version}")
    distance, knn = dataset.get("distance", 0), dataset.get("knn", 0)
    LOG.info(f"Spatially Joining: {dataset_parcel['name']} with {dataset['name']} at {distance=}m\nOutput: {dataset['lookup_parcel']}")
    sdf_parcel = load_sdf(dataset_parcel["silver"])
    sdf_other = load_sdf(dataset["silver"])
    if not os.path.exists(dataset["lookup_parcel"]):
        sdf = sjoin(
            sdf_parcel.selectExpr("id_parcel", "geometry"),
            sdf_other.select("fid", "geometry"),
            distance = distance, knn = knn,
        ).select("id_parcel", "fid").drop_duplicates()
        sdf.write.parquet(dbfs(dataset["lookup_parcel"], True))
    dataset["tasks"]["lookup_parcel"] = datetime.today().strftime("%Y_%m_%d")
    return dataset

# COMMAND ----------

dataset = find_datasets("elmo_geo-water")[-1]
lookup_parcel(dataset)

# COMMAND ----------

run_task_on_catalogue("lookup_parcel", lookup_parcel)

# COMMAND ----------

# dataset = find_datasets('elmo_geo-water')[-1]
# lookup_parcel(dataset)
