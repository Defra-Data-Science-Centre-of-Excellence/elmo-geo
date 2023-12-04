# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Tree Features
# MAGIC
# MAGIC Objective: Produce parcel level metrics releated to the presence of trees.
# MAGIC
# MAGIC This notebook takes data on tree locations, parcel boundaries, and hedgerow locations to produce parcel tree metrics.
# MAGIC
# MAGIC ## Input Data
# MAGIC
# MAGIC Tree locations and crowns are taken from the output of the VOM tree detection notebook (), saved at
# MAGIC `dbfs:/mnt/lab/unrestricted/elm/elmo/hrtrees/tree_detections/tree_detections_{timestamp}.parquet`
# MAGIC
# MAGIC Parcel geometries are taken from
# MAGIC `dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2021_03_16.parquet`.
# MAGIC Using the March 2021 geometries to best align with the version of the WFM being used in elmo (May 2021) as of Nov 2023.
# MAGIC
# MAGIC Hedgerow geometries are already linked to parcels and located at
# MAGIC `dbfs:/mnt/lab/unrestricted/elm_data/rural_payments_agency/efa_hedges/2022_06_24.parquet`
# MAGIC
# MAGIC Hedgerow lengths are taken from
# MAGIC `dbfs:/mnt/lab/unrestricted/elm/elmo/hedgerows_and_water/hedgerows_and_water.csv`
# MAGIC
# MAGIC Water body geometries taken from
# MAGIC `dbfs:/mnt/lab/unrestricted/elm/buffer_strips/waterbody_geometries.parquet `
# MAGIC
# MAGIC ## Output Data
# MAGIC
# MAGIC > Tree Features
# MAGIC > This table has the tee features for each parcel.
# MAGIC > `dbfs:/mnt/lab/unrestricted/elm/elmo/hrtrees/tree_features_{timestamp}.parquet`
# MAGIC > The timestamp of the file path corresponds to the timestamp of the tree detection data used as an input to the notebook.
# MAGIC >
# MAGIC | SHEET_ID | PARCEL_ID | perimeter_length | SHEET_PARCEL_ID | hrtrees_count2 | wbtrees_count2  | wbtrees_count4 | perim_trees_count2 | crown_perim_length2 | int_trees_count2 | perim_trees_count4 | crown_perim_length4 | int_trees_count4
# MAGIC |---|---|---|---|---|---|---|---|---|---|---|---|---|
# MAGIC |   |   |   |   |   |   |   |   |   |   |   |   |   |
# MAGIC
# MAGIC ### Fields Metadata
# MAGIC
# MAGIC ##### SHEET_ID
# MAGIC
# MAGIC The parcel sheet ID
# MAGIC
# MAGIC ##### PARCEL_ID
# MAGIC
# MAGIC The parcel parcel ID
# MAGIC
# MAGIC ##### perimeter_length
# MAGIC
# MAGIC The length of the parcel perimeter. Calculated by getting the length of the boundary of the parcel polygon
# MAGIC
# MAGIC ##### SHEET_PARCEL_ID
# MAGIC
# MAGIC Unique identifier for each parcel. Not duplicated.
# MAGIC
# MAGIC Produced by concatenating REF_PARCEL_SHEET_ID and REF_PARCEL_PARCEL_ID
# MAGIC
# MAGIC ##### hrtrees_count{buffer}
# MAGIC
# MAGIC Number of hedgerow trees associated with this parcel.
# MAGIC
# MAGIC Given by intersecting buffered hedgerow geometries with tree crown coordinates.
# MAGIC
# MAGIC The amount hedgerows are buffered by is given in the field name. hr_tree_count2 means the hedgerow geometry was buffered 2m before intersecting with the trees.
# MAGIC
# MAGIC ##### wbtrees_count{buffer}
# MAGIC
# MAGIC Number of water body trees associated with this parcel.
# MAGIC
# MAGIC Given by intersecting buffered water body geometries with tree crown coordinates.
# MAGIC
# MAGIC Water bodies are filtered to exclude geometries tagged as ‘Sea’.
# MAGIC
# MAGIC The amount water bodies are buffered by is indicated by the number in the field name.
# MAGIC
# MAGIC ##### perim_trees_count{buffer}
# MAGIC
# MAGIC Number of trees that intersect with the parcel’s perimeter.
# MAGIC
# MAGIC The distance the parcel perimeter is buffered by is given in the field name.
# MAGIC
# MAGIC Here is a figure showing perimeter trees (in green) for a few parcels using a 2m buffer:
# MAGIC
# MAGIC ##### crown_perim_length{buffer}
# MAGIC
# MAGIC The length of the parcel perimeter that intersects with perimeter tree crowns.
# MAGIC
# MAGIC Calculated by intersecting the crown geometries (polygons) of perimeter trees (trees whose crown coordinate intersects with the buffered perimeter) with the parcel perimeter. See figure above for illustration.
# MAGIC
# MAGIC ##### int_trees_count{buffer}
# MAGIC
# MAGIC Number of trees in the parcel interior.
# MAGIC
# MAGIC The parcel interior is given by the difference between the buffered parcel perimeter and the parcel geometry. The distance the parcel perimeter is buffered by is given in the field name.
# MAGIC

# COMMAND ----------

# MAGIC %pip install rich

# COMMAND ----------

from tree_features import *

#from sedona.spark import SedonaContext
from sedona.register import SedonaRegistrator
from elmo_geo.utils.dbr import spark

# COMMAND ----------

#SedonaContext.create(spark)
SedonaRegistrator.registerAll(spark)

# COMMAND ----------

hedgerowBufferDistances = [2]
parcelBufferDistances = [2, 4]
waterbodyBufferDistances = [2, 4]

timestamp = "202311231323"  # 202311231323 timestamp is tree detection version with best F1 score so far.

hedgerows_path = (
    "dbfs:/mnt/lab/unrestricted/elm_data/rural_payments_agency/efa_hedges/2022_06_24.parquet"
)

waterbodies_path = "dbfs:/mnt/lab/restricted/ELM-Project/out/water.parquet"

hedges_length_path = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/hedgerows_and_water/hedgerows_and_water.csv"
)

parcels_path = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2021_03_16.parquet"

trees_output_template = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/"
    "tree_features/tree_detections/"
    "tree_detections_{timestamp}.parquet"
)
output_trees_path = trees_output_template.format(timestamp=timestamp)

features_output_template = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/tree_features_{timestamp}.parquet"
)
parcel_trees_output = features_output_template.format(timestamp=timestamp)

# COMMAND ----------

parcel_trees_output

# COMMAND ----------

output_trees_path

# COMMAND ----------

# Just SP and SU major tiles
# timestamp = "202306071149"
# output_trees_path = f"dbfs:/mnt/lab/unrestricted/elm/elmo/hrtrees/tree_detections/SP_SU_tree_detections_{timestamp}.parquet"
# parcel_trees_output = f"dbfs:/mnt/lab/unrestricted/elm/elmo/hrtrees/SP_tree_features_{timestamp}.parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Load data

# COMMAND ----------

treesDF = spark.read.parquet(output_trees_path)
parcelsDF = spark.read.parquet(parcels_path)
hrDF = spark.read.parquet(hedgerows_path)
#wbDF = spark.read.parquet(waterbodies_path)
wbDF = None

# COMMAND ----------

treesDF.count(), hrDF.count(), parcelsDF.count()

# COMMAND ----------

treesDF = treesDF.repartition(100_000)
hrDF = hrDF.repartition(1_000)
parcelsDF = parcelsDF.repartition(100)

# COMMAND ----------

# Filter to a single tile, for testing
# tile_to_visualise = "SP65nw"
# tile_to_visualise = "SP"
# treesDF = treesDF.filter(f"chm_path like '%{tile_to_visualise}%'")
# parcelsDF = parcelsDF.filter(f"SHEET_ID like '{tile_to_visualise[:2]}%'")
# hrDF = hrDF.filter(f"REF_PARCEL_SHEET_ID like '{tile_to_visualise[:2]}%'")
# wbDF = wbDF.filter(f"id_parcel like '{tile_to_visualise[:2]}%'")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Get tree features

# COMMAND ----------

pTreesDF = get_parcel_tree_features(
    spark,
    treesDF,
    parcelsDF,
    hrDF,
    wbDF,
    parcelBufferDistances,
    hedgerowBufferDistances,
    waterbodyBufferDistances,
    double_count=True,
)

# COMMAND ----------

parcel_trees_output

# COMMAND ----------

# Save the data
pTreesDF.write.mode("overwrite").parquet(parcel_trees_output)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Check data

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC ls "/dbfs/mnt/lab/unrestricted/elm/elmo/tree_features/"

# COMMAND ----------

parcels_path

# COMMAND ----------

sdf_parcels = spark.read.parquet(parcels_path)
sdf_tf_old = spark.read.parquet(parcel_trees_output.replace("202311231323", "202308032321"))
sdf_tf_new = spark.read.parquet(parcel_trees_output)

# COMMAND ----------

sdf_tf_new.createOrReplaceTempView("tf")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tf

# COMMAND ----------

parcel_ids = sdf_parcels.select("SHEET_ID", "PARCEL_ID").toPandas()
tf_old_ids = sdf_tf_old.select("SHEET_PARCEL_ID").toPandas()
tf_new_ids = sdf_tf_new.select("SHEET_PARCEL_ID").toPandas()

# COMMAND ----------

import pandas as pd

# COMMAND ----------

parcel_ids['SHEET_PARCEL_ID'] = parcel_ids['SHEET_ID'] + parcel_ids['PARCEL_ID']

parcel_ids = pd.merge(parcel_ids, tf_old_ids, on = 'SHEET_PARCEL_ID', suffixes = ("", "_old"), indicator=True, how = 'outer').rename(columns = {'_merge':'old_merge'})

parcel_ids = pd.merge(parcel_ids, tf_new_ids, on = 'SHEET_PARCEL_ID', suffixes = ("", "_new"), indicator=True, how = 'outer').rename(columns = {'_merge':'new_merge'})

# COMMAND ----------

parcel_ids['old_merge'].value_counts()

# COMMAND ----------

parcel_ids['new_merge'].value_counts()
