# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Checking PHI Totals
# MAGIC
# MAGIC Checking the total area of PHI geometries and the total area on parcels.

# COMMAND ----------

import pyspark.sql.functions as F

from elmo_geo.datasets import (
    defra_priority_habitat_england_raw,
    defra_priority_habitat_parcels,
    reference_parcels,
)

from elmo_geo.io.file import auto_repartition
from elmo_geo.st.join import sjoin
from elmo_geo import register

register()

# COMMAND ----------

defra_priority_habitat_england_raw.sdf().display()

# COMMAND ----------

# total area of raw phi geometries - 2,294,282
(defra_priority_habitat_england_raw.sdf()
 .dropDuplicates(subset=["geometry"])
 .agg(F.sum("areaha")).display())

# COMMAND ----------

# area of phi on parcels - 1,758,555 ha
(reference_parcels.sdf().join(
    defra_priority_habitat_parcels.sdf().dropDuplicates(subset=["id_parcel", "fid"]), 
    on="id_parcel", 
    how="left",)
 .withColumn("phi_area", F.expr("proportion*area_ha"))
 .agg(F.sum("phi_area")).display())

# COMMAND ----------

# check that phi geometries don't overlap
# expect query to return zero (or small) area
# result - 932 ha
(sjoin(defra_priority_habitat_england_raw.sdf(), defra_priority_habitat_england_raw.sdf())
 .filter("fid_left <> fid_right")
 .transform(auto_repartition, count_ratio=1e-5)
 .withColumn("geometry_left", F.expr("ST_CollectionExtract(geometry_left, 3)"))
 .withColumn("geometry_right", F.expr("ST_CollectionExtract(geometry_right, 3)"))
 .withColumn("ha_intersection", 
             F.expr("ST_Area(ST_Intersection(geometry_left, geometry_right)) / 10000"))
 .agg(F.sum("ha_intersection"))
).display()

# COMMAND ----------

# checking overlap between geometries with the same id - approximately matches the phi total 2,294,282 vs 2,294,127
(sjoin(defra_priority_habitat_england_raw.sdf(), defra_priority_habitat_england_raw.sdf())
 .filter("fid_left = fid_right")
 .transform(auto_repartition, count_ratio=1e-5)
 .withColumn("geometry_left", F.expr("ST_CollectionExtract(geometry_left, 3)"))
 .withColumn("geometry_right", F.expr("ST_CollectionExtract(geometry_right, 3)"))
 .withColumn("ha_intersection", 
             F.expr("ST_Area(ST_Intersection(geometry_left, geometry_right)) / 10000"))
 .agg(F.sum("ha_intersection"))
).display()
