# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Test Sjoin
# MAGIC
# MAGIC **Author:** Obi Thompson Sargoni
# MAGIC
# MAGIC Testing that the elmo-geo sjoin functions works correctly when joining a dataset to itself

# COMMAND ----------

import os
import geopandas as gpd
from pyspark.sql import functions as F
from shapely.geometry import Point
import pandas as pd

from elmo_geo import LOG, register
from elmo_geo.st import sjoin

register()

# COMMAND ----------

# DBTITLE 1,Define Test Function
def test_self_sjoin(sdf):
    """Tests intersecting a spark data frame with itself.

    Expect all rows to have an intersecting geometry.
    """
    sdf_joined = sjoin(sdf, sdf, how = "inner")
    count_no_intersection = sdf_joined.filter(F.expr("ST_IsEmpty(ST_Intersection(geometry_left, geometry_right))")).count()
    return count_no_intersection==0

# COMMAND ----------

# DBTITLE 1,Define geometries
# create the test dataframe
n = 1_000
geoms = [Point(i,i).wkt for i in range (n)]
df = pd.DataFrame({"id":range(n), "geometry":geoms})
df.head()

# COMMAND ----------

# DBTITLE 1,Test passes without additional ID field
# The test passes when a spark data frame is created without using F.monotinically_increasing_id()
sdf = (spark.createDataFrame(df)
       .withColumn("geometry", F.expr("ST_GeomFromText(geometry)"))
)
LOG.info(f"Test passes: {test_self_sjoin(sdf)}")

# COMMAND ----------

# DBTITLE 1,Test passes with additional ID field and no repartition
# creating another ID with monotonically_increasing_id() works if the data is not re-partitioned
sdf = (spark.createDataFrame(df)
       .withColumn("geometry", F.expr("ST_GeomFromText(geometry)"))
       .withColumn("id2", F.monotonically_increasing_id())
)

LOG.info(f"Test passes: {test_self_sjoin(sdf)}")

# COMMAND ----------

# DBTITLE 1,Test fails if dataframe is re-partitioned
sdf = (spark.createDataFrame(df)
       .withColumn("geometry", F.expr("ST_GeomFromText(geometry)"))
       .withColumn("id2", F.monotonically_increasing_id())
       .repartition(10)
)
LOG.info(f"Test passes: {test_self_sjoin(sdf)}")

# COMMAND ----------

# DBTITLE 1,Test passes if ID creation is executed before re-partitioning
sdf = (spark.createDataFrame(df)
       .withColumn("geometry", F.expr("ST_GeomFromText(geometry)"))
       .withColumn("id2", F.monotonically_increasing_id())
)

sdf.write.format("noop").mode("overwrite").save()
sdf.repartition(10)
LOG.info(f"Test passes: {test_self_sjoin(sdf)}")
