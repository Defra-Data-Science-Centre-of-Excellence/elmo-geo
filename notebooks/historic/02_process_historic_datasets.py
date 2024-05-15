# Databricks notebook source
# MAGIC %md
# MAGIC # Intersect historic and archaaeological features with parcels 
# MAGIC **Author:** Obi Thompson Sargoni
# MAGIC
# MAGIC **Date:** 13-05-2024
# MAGIC
# MAGIC This notebook loads the outputs of `01_combine_historic_datasets` and intersects these with parcels.
# MAGIC
# MAGIC It is based on the `proces_datasets` notebook but is different in the following ways:
# MAGIC - combines the intersection prportions of three different historic dfeatures datasets into a single output dataset
# MAGIC - buffers the historic geometries by 0m and 6m and calcualtes the proportion for each
# MAGIC
# MAGIC To do:
# MAGIC - intersect features with parcel boundaries and calculate proportion of boudnary

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import os
import geopandas as gpd
from functools import partial
from pyspark.sql import functions as F

from elmo_geo import LOG, register
from elmo_geo.datasets.datasets import datasets, parcels
from elmo_geo.io import download_link
from elmo_geo.io.preprocessing import geometry_to_wkb, make_geometry_valid, transform_crs
from elmo_geo.st import sjoin
from elmo_geo.st.geometry import load_geometry
from elmo_geo.utils.misc import dbfs

register()

# COMMAND ----------

parcels_names = sorted([f"{parcels.source}/{parcels.name}/{v.name}" for v in parcels.versions])
dbutils.widgets.dropdown("parcels", "rpa/parcels/2021_11_16_adas", parcels_names)
_, pname, pversion = dbutils.widgets.get("parcels").split("/")
[print("name", parcels.name, sep=":\t"), 
 print("version", next(v for v in parcels.versions if v.name == pversion), sep = ":\t"),
 ]
path_parcels = next(v.path_read for v in parcels.versions if v.name == pversion)

version = "2024_04_29"
he_combined_dataset = next(d for d in datasets if d.name == "historic_archaeological")
he_scheduled_monuments_dataset = next(d for d in datasets if d.name == "scheduled_monuments")

sf_combined_sites = next(v for v in he_combined_dataset.versions if v.name==version).path_read
sf_scheduled_monuments = next(v for v in he_scheduled_monuments_dataset.versions if v.name==version).path_read
print(f"\n\nHistoric features paths:\n{sf_combined_sites}\n{sf_scheduled_monuments}")

target_epsg = 27700
simplify_tolerence: float = 0.5  # metres
max_vertices: int = 256  # per polygon (row)

date = "2024_05_03"
file_name = f"he-historic_features-{date}"
sf_output_historic_features = f"dbfs:/mnt/lab/restricted/ELM-Project/out/{file_name}.parquet"

# COMMAND ----------

# process the parcels dataset to ensure validity, simplify the vertices to a tolerence,
# and subdivide large geometries
df_parcels = (
    spark.read.format("geoparquet").load(path_parcels)
    .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
    .withColumn("geometry", F.expr(f"ST_SimplifyPreserveTopology(geometry, {simplify_tolerence})"))
    .withColumn("geometry", F.expr("ST_Force_2D(geometry)"))
    .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
    .select("id_parcel", "geometry")
)
df_parcels.display()

# COMMAND ----------

# load each of the historic england datasets
sdf_combined_sites = (spark.read.format("parquet").load(dbfs(sf_combined_sites, True))
                      .withColumn("geometry", load_geometry("geometry"))
)
sdf_combined_sites.display()

sdf_scheduled_monuments = (spark.read.format("parquet").load(dbfs(sf_scheduled_monuments, True))
                           .withColumn("geometry", load_geometry("geometry"))
                           .withColumn("dataset", F.lit("scheduled_monuments"))
                           .withColumnRenamed("ListEntries", "ListEntry")
)
sdf_scheduled_monuments.display()

historic_datasets = {
    "hist_arch": sdf_combined_sites,
    "scheduled_monuments": sdf_scheduled_monuments,
    "hist_arch_ex_sched_monuments": sdf_combined_sites.filter("dataset != 'scheduled_monuments'"),
}

# COMMAND ----------

# join the two datasets and calculate the proportion of the parcel that intersects
sdf_historic_features = None
collect = lambda col: F.array_join(F.array_sort(F.collect_set(col)), "-")

for buf in [0, 6]:
    buf_suffix = f"_{buf}m" if buf !=0 else ""
    for name, sdf_feature in historic_datasets.items():
        print(f"Dataset:{name}\nbuffer:{buf}\n")
        prop_col = f"prop_{name}{buf_suffix}"
        df = (
            sjoin(df_parcels, 
                  sdf_feature.withColumn("geometry", F.expr(f"ST_MakeValid(ST_Buffer(geometry, {buf}))")),
                  )
            .groupBy(["id_parcel", "geometry_left"])
            .agg(
                # combine overlapping geometries into single multi geometry and concatenate the datasets and listentry ids
                collect("dataset").alias(f"sources_{name}{buf_suffix}"),
                F.expr("ST_Union_Aggr(geometry_right) as geometry_right"),
            )
            .withColumn("geometry_intersection", F.expr("ST_Intersection(geometry_left, geometry_right)"))
            .withColumn("area_left", F.expr("ST_Area(geometry_left)"))
            .withColumn("area_intersection", F.expr("ST_Area(geometry_intersection)"))
            .withColumn(prop_col, F.col("area_intersection") / F.col("area_left"))
            .drop("area_left", "area_intersection", "geometry_left", "geometry_right", "geometry_intersection")
        )
        # group up the result and sum the proportions
        df = (
            df.groupBy(*[col for col in df.columns if col != prop_col])
            .sum(prop_col)
            .withColumn(prop_col, F.round(f"sum({prop_col})", 6))
            .where(f"{prop_col} > 0")
            .drop(f"sum({prop_col})")
        )

        if sdf_historic_features is None:
            sdf_historic_features = df
        else:
            sdf_historic_features = sdf_historic_features.join(df, on="id_parcel", how = "outer")

prop_cols = [i for i in sdf_historic_features.columns if "prop_" in i]
sdf_historic_features = sdf_historic_features.fillna(0, subset=prop_cols)

# COMMAND ----------

# save the data
(   
    sdf_historic_features
    .write.format("parquet")
    .mode("overwrite")
    .save(dbfs(sf_output_historic_features, True))
)

# COMMAND ----------

# show results
result = spark.read.parquet(dbfs(sf_output_historic_features, True))
count = result.count()
LOG.info(f"Rows: {count:,.0f}")
# check proportion is never > 1 - if it is might mean duplicate features int he dataset
pandas_df = result.toPandas()

proportion_columns = [i for i in pandas_df.columns if "prop_" in i]
for pc in proportion_columns:
    proportion_over_1 = (pandas_df[pc] > 1.0).sum()
    if proportion_over_1:
        LOG.info(f"Column {pc}:\n{proportion_over_1:,.0f} parcels have a feature overlapping by a proportion > 1 ({proportion_over_1/count:%})\n\n")
result.display()

# COMMAND ----------

download_path = f"/dbfs/FileStore/{file_name}.parquet"
pandas_df.to_parquet(download_path)
displayHTML(download_link(dbfs(download_path, False), name = "download.parquet"))

# COMMAND ----------

import pandas as pd
df = pd.read_parquet(dbfs(download_path, False))
df

# COMMAND ----------

df["id_parcel"].nunique()
