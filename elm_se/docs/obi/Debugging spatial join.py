# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Relevant documentation: https://sedona.apache.org/1.3.1-incubating/tutorial/core-python/#create-a-spatialrdd
# MAGIC https://sedona.apache.org/1.4.0/tutorial/sql/?h=carryotherattributes#spatialrdd-to-dataframe
# MAGIC
# MAGIC StackOverflow question: https://stackoverflow.com/questions/76526032/pyspark-sedona-how-to-retain-all-fields-from-input-sparkdataframes-when-perfor

# COMMAND ----------

import itertools
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely import from_wkt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sedona.register import SedonaRegistrator
from sedona.utils.adapter import Adapter
from sedona.core.spatialOperator import JoinQueryRaw

import matplotlib.pyplot as plt

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
SedonaRegistrator.registerAll(spark)

# COMMAND ----------

def _get_column_names(df_left: SparkDataFrame, df_right: SparkDataFrame) -> List[str]:
    """Generate column names for the DataFrame resulting from a join
    Parameters:
        df_parcels: The parcels dataset
        df_features: The features dataset
    Returns:
        A list of column names
    Raises:
        KeyError: When either DataFrame is missing a column called `geometry`
    """
    for df in [df_left, df_right]:
        if "geometry" not in (cols := [f.name for f in df.schema]):
            raise KeyError(
                "Could not find `geometry` column in DataFrame. "
                f"Found the following columns: {cols}"
            )
    cols_parcels = ["geom_left"] + [f.name for f in df_left.schema if f.name != "geometry"]
    cols_features = ["geom_right"] + [f.name for f in df_right.schema if f.name != "geometry"]
    return cols_parcels + cols_features

def _get_combined_schema(df_left: SparkDataFrame, df_right: SparkDataFrame) -> StructType:
    names_left, names_right = _get_column_names(df_left, df_right)
    column_names = names_left+names_right

    combined_schema = []
    for n in column_names:
        for s in df_left.schema:
            if s.name == n:
                combined_schema.append(s)
            elif (s.name=="geometry") & (n=="geom_left"):
                s_ = StructField(name = n, dataType = s.dataType, nullable= s.nullable)
                combined_schema.append(s_)
        for s in df_right.schema:
            if s.name == n:
                combined_schema.append(s)
            elif (s.name=="geometry") & (n=="geom_right"):
                s_ = StructField(name = n, dataType = s.dataType, nullable= s.nullable)
                combined_schema.append(s_)
    return StructType(combined_schema)

# COMMAND ----------

# Define spatal data frames
coords = zip(np.random.choice(range(10), 10), np.random.choice(range(10), 10))
points = [ Point(i) for i in coords]
circles = [p.buffer(2) for p in points]
circles_wkt = [c.wkt for c in circles]

gdf1 = gpd.GeoDataFrame({"id1":range(5), "wkt": circles_wkt[:5], "geometry":circles[:5]}, geometry="geometry")
gdf2 = gpd.GeoDataFrame({"id2":range(5,10), "wkt": circles_wkt[5:], "geometry":circles[5:]}, geometry="geometry")

# COMMAND ----------

# plot the data
f, ax = plt.subplots(figsize=(10, 10))
gdf1.plot(ax=ax, facecolor='red', alpha=0.5)
gdf2.plot(ax=ax, facecolor='blue', alpha=0.5)

# COMMAND ----------

# convert to spark dataframes
sdf1 = spark.createDataFrame(gdf1[['wkt', 'id1']])
sdf2 = spark.createDataFrame(gdf2[['wkt', 'id2']])

# COMMAND ----------

sdf1 = sdf1.withColumn("geometry", F.expr("ST_GeomFromWKT(wkt)"))
sdf2 = sdf2.withColumn("geometry", F.expr("ST_GeomFromWKT(wkt)"))

# COMMAND ----------

# convert to RDD
rdd_left = Adapter.toSpatialRdd(sdf1, "geometry")
rdd_right = Adapter.toSpatialRdd(sdf2, "geometry")
rdd_left.analyze()
rdd_right.analyze()

# COMMAND ----------

rdd_left.rawSpatialRDD.toDF(schema = sdf1.schema)

#object_rdd.rawSpatialRDD.map(lambda x: x.getUserData())

# COMMAND ----------

# join
rdd_left.spatialPartitioning(partitioning="KDBTREE", num_partitions=1)
rdd_right.spatialPartitioning(rdd_left.getPartitioner())


result_pair_rdd = JoinQueryRaw.SpatialJoinQueryFlat(
    rdd_right,
    rdd_left,
    useIndex=False,
    considerBoundaryIntersection=False
)

# COMMAND ----------

names_left, names_right = _get_column_names(sdf1, sdf2)
cs = _get_combined_schema(sdf1, sdf1)

# COMMAND ----------

Adapter.toDf(rdd_left, spark).columns

# COMMAND ----------

# Convert to DF
df = Adapter.toDf(result_pair_rdd, spark)
df.columns

# COMMAND ----------

df = Adapter.toDf(result_pair_rdd, spark).toDF(*_get_column_names(sdf1, sdf2))

# COMMAND ----------

rddl_with_other_attributes = rdd_left.rawSpatialRDD.map(lambda x: x.getUserData())
rddr_with_other_attributes = rdd_right.rawSpatialRDD.map(lambda x: x.getUserData())

# COMMAND ----------

rddl_with_other_attributes.toDF(schema=sdf1.schema)

# COMMAND ----------

result_pair_rdd2 = JoinQueryRaw.SpatialJoinQueryFlat(
    rddr_with_other_attributes,
    rddl_with_other_attributes,
    useIndex=False,
    considerBoundaryIntersection=False,
)

df = Adapter.toDf(result_pair_rdd2, spark)

# COMMAND ----------


