# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Test Sjoin
# MAGIC
# MAGIC **Author:** Obi Thompson Sargoni
# MAGIC
# MAGIC Highlighting a scenario in while sjoin fails.
# MAGIC
# MAGIC Fail case: when the input spark dataframe has an F.monotonically_increasing_id() field and is re-partitioned.

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from shapely.geometry import Point

from elmo_geo import LOG, register
from elmo_geo.st import sjoin
from elmo_geo.utils.misc import cache

register()

# COMMAND ----------

# DBTITLE 1,Define Test Function


def test_self_sjoin(sdf):
    """Tests intersecting a spark data frame with itself.

    Expect all rows to have an intersecting geometry.
    """
    sdf_joined = sjoin(sdf, sdf, how="inner")
    count_no_intersection = sdf_joined.filter("NOT ST_Intersects(geometry_left, geometry_right)").count()
    return count_no_intersection == 0


# COMMAND ----------

# DBTITLE 1,Define geometries
# create the test dataframe
n = 1_000
geoms = [Point(i, i).wkt for i in range(n)]
df = pd.DataFrame({"id_orig": range(n), "geometry": geoms})
df.head()

# COMMAND ----------

# DBTITLE 1,Test passes without additional ID field
# The test passes when a spark data frame is created without using F.monotinically_increasing_id()
sdf = spark.createDataFrame(df).withColumn("geometry", F.expr("ST_GeomFromText(geometry)"))
LOG.info(f"Test passes: {test_self_sjoin(sdf)}")

# COMMAND ----------

# DBTITLE 1,Test passes with additional ID field and no repartition
# creating another ID with monotonically_increasing_id() works if the data is not re-partitioned
sdf = spark.createDataFrame(df).withColumn("geometry", F.expr("ST_GeomFromText(geometry)")).withColumn("id2", F.monotonically_increasing_id())

LOG.info(f"Test passes: {test_self_sjoin(sdf)}")

# COMMAND ----------

# DBTITLE 1,Test fails if dataframe is re-partitioned
sdf = spark.createDataFrame(df).withColumn("geometry", F.expr("ST_GeomFromText(geometry)")).withColumn("id2", F.monotonically_increasing_id()).repartition(1)
LOG.info(f"Test passes: {test_self_sjoin(sdf)}")

sdfj = sjoin(sdf, sdf, how="inner")
print(sdfj.count())
sdfj.display()

# COMMAND ----------

# DBTITLE 1,Test passes if ID creation is executed before re-partitioning
sdf = spark.createDataFrame(df).withColumn("geometry", F.expr("ST_GeomFromText(geometry)")).withColumn("id2", F.monotonically_increasing_id())

sdf.write.format("noop").mode("overwrite").save()
sdf.repartition(10)
LOG.info(f"Test passes: {test_self_sjoin(sdf)}")

# COMMAND ----------

# fails with cache, even though this is the same as line 6 above
sdf = (
    spark.createDataFrame(df)
    .withColumn("geometry", F.expr("ST_GeomFromText(geometry)"))
    .withColumn("id2", F.monotonically_increasing_id())
    .transform(cache)
    .repartition(10)
)
LOG.info(f"Test passes: {test_self_sjoin(sdf)}")

# COMMAND ----------

# passes with cache not transformed
sdf = spark.createDataFrame(df).withColumn("geometry", F.expr("ST_GeomFromText(geometry)")).withColumn("id2", F.monotonically_increasing_id())

sdf = cache(sdf)
sdf.repartition(10)
LOG.info(f"Test passes: {test_self_sjoin(sdf)}")

# COMMAND ----------

# passes with coalesce rather than repartition
sdf = spark.createDataFrame(df).withColumn("geometry", F.expr("ST_GeomFromText(geometry)")).withColumn("id2", F.monotonically_increasing_id()).coalesce(1)
LOG.info(f"Test passes: {test_self_sjoin(sdf)}")

sdfj = sjoin(sdf, sdf, how="inner")
print(sdfj.count())
sdfj.display()

# COMMAND ----------

# fails with repartition before id, even with cache
sdf = (
    spark.createDataFrame(df)
    .withColumn("geometry", F.expr("ST_GeomFromText(geometry)"))
    .repartition(10)
    .withColumn("id2", F.monotonically_increasing_id())
    .transform(cache)
)
LOG.info(f"Test passes: {test_self_sjoin(sdf)}")
