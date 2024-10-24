# Databricks notebook source
# MAGIC %md
# MAGIC # Priority Habitat - Heathland Proximity
# MAGIC
# MAGIC This notebook filters the Priority Habitat dataset to contain only priority habitat healthland data.
# MAGIC It then calculates the minimum distance from each parcel
# MAGIC
# MAGIC **Author:** Obi Thompson Sargoni
# MAGIC
# MAGIC **Date:** 16/04/2024
# MAGIC
# MAGIC This notebook produces a parcel level dataset which gives the minimum distance from each parcel to features in the input features datasets,
# MAGIC upto a maximum threshold distance.

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F

from elmo_geo import LOG, register
from elmo_geo.io import download_link
from elmo_geo.st.join import knn
from elmo_geo.st.udf import st_clean

register()

# COMMAND ----------

sf_parcels = "dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet"
sf_priority_habitat = "dbfs:/mnt/lab/unrestricted/elm_data/defra/priority_habitat_inventory/unified_2021_03_26.parquet"

groupby_variable = "heath_habitat"

date = "2024-04-16"
dataset_name = "priority_habitat_inventory_heathland_proximity"
sf_heathland_proximity = f"dbfs:/mnt/lab/restricted/ELM-Project/out/defra-{dataset_name}-{date}.parquet"

n_partitions = 200
max_vertices: int = 256  # per polygon (row)
DISTANCE_THRESHOLD = 5_000

# COMMAND ----------

# take a look at the processed data
spark.read.parquet(sf_priority_habitat).display()

# COMMAND ----------

# process the parcels dataset to ensure validity, simplify the vertices to a tolerance,
# and subdivide large geometries
df_parcels = spark.read.format("geoparquet").load(sf_parcels).transform(st_clean).select("id_parcel", "geometry").repartition(1_000)
df_parcels.display()

# COMMAND ----------

# process the feature dataset to ensure validity, simplify the vertices to a tolerance,
# and subdivide large geometries
df_feature = (
    spark.read.parquet(sf_priority_habitat)
    .filter(F.expr("Main_Habit like '%heath%'"))
    .withColumn("geometry", F.expr("ST_GeomFromWKB(geometry)"))
    .withColumn("geometry", F.expr(f"ST_SubdivideExplode(geometry, {max_vertices})"))
    .transform(st_clean)
    .repartition(n_partitions)
    .select(
        F.col("Main_Habit").alias(groupby_variable),
        "geometry",
    )
)

df_feature.display()

# COMMAND ----------

sdf = knn(df_parcels, df_feature, id_left="id_parcel", id_right=groupby_variable, k=1, distance_threshold=DISTANCE_THRESHOLD)
sdf.write.format("parquet").save(sf_heathland_proximity, mode="overwrite")

# COMMAND ----------

# view the data
spark.read.format("parquet").load(sf_heathland_proximity).display()

# COMMAND ----------

# show results
result = spark.read.parquet(sf_heathland_proximity)
count = result.count()
LOG.info(f"Rows: {count:,.0f}")

# COMMAND ----------

result.toPandas().groupby(groupby_variable).distance.describe()

# COMMAND ----------

# download
pandas_df = result.toPandas()
path_parquet = "/dbfs" + sf_heathland_proximity.replace(dataset_name, dataset_name + "_output").replace("dbfs:", "")

# output
pandas_df.to_parquet(path_parquet)
displayHTML(download_link(path_parquet))

# COMMAND ----------

df = pd.read_parquet("/dbfs" + sf_heathland_proximity.replace(dataset_name, dataset_name + "_output").replace("dbfs:", ""))
df

# COMMAND ----------

df["id_parcel"].nunique()
