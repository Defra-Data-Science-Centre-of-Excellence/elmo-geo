# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Combine Historic and Archaeological Datasets
# MAGIC **Author:** Obi Thompson Sargoni
# MAGIC
# MAGIC **Date:** 13-05-2024
# MAGIC
# MAGIC This notebook combines multiple sources of historic and archaeological features into a single parquet file.
# MAGIC
# MAGIC This enables the proportion of parcels intersected by historic or archaeological features to be calculated, though this is done in a separate notebooks.

# COMMAND ----------

from pyspark.sql import functions as F

from elmo_geo import register
from elmo_geo.st.udf import st_clean
from elmo_geo.utils.misc import dbfs

register()

# COMMAND ----------

f_protected_wreck_sites = "/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_protected_wreck_sites/format_GEOPARQUET_protected_wreck_sites/SNAPSHOT_2024_04_29_protected_wreck_sites/"  # noqa:E501
f_registered_battlefields = "/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_registered_battlefields/format_GEOPARQUET_registered_battlefields/SNAPSHOT_2024_04_29_registered_battlefields/"  # noqa:E501
f_registered_parks_and_gardens = "/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_registered_parks_and_gardens/format_GEOPARQUET_registered_parks_and_gardens/SNAPSHOT_2024_04_29_registered_parks_and_gardens/"  # noqa:E501
f_scheduled_monuments = "/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_scheduled_monuments/format_GEOPARQUET_scheduled_monuments/SNAPSHOT_2024_04_29_scheduled_monuments/"  # noqa:E501
f_world_heritage_sites = "/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_world_heritage_sites/format_GEOPARQUET_world_heritage_sites/SNAPSHOT_2024_04_29_world_heritage_sites/"  # noqa:E501
f_listed_buildings = "/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_listed_buildings_polys/format_GEOPARQUET_listed_buildings_polys/SNAPSHOT_2024_05_03_listed_buildings_polys/"  # noqa:E501

f_shine = "/dbfs/mnt/lab-res-a1001004/restricted/elm_project/bronze/he-shine-2022_12_30.parquet"

date = "2024_05_03"
f_output_historic_combined = f"/dbfs/mnt/lab-res-a1001004/restricted/elm_project/stg/he-combined_sites-{date}.parquet"

filepaths_he = {
    # "listed_building": f_listed_buildings,  # TODO: #126
    "protected_wreck_sites": f_protected_wreck_sites,
    "registered_battlefields": f_registered_battlefields,
    "registered_parks_and_gardens": f_registered_parks_and_gardens,
    "scheduled_monuments": f_scheduled_monuments,
    "world_heritage_sites": f_world_heritage_sites,
}

# COMMAND ----------

sdf_historical_sites = spark.read.parquet(dbfs(f_shine, True)).selectExpr(
    "'SHINE' AS dataset",
    "shine_uid AS ListEntry",
    "shine_name AS Name",
    "geom AS geometry",
)

for dataset, p in filepaths_he.items():
    sdf_historical_sites = sdf_historical_sites.unionByName(
        spark.read.parquet(dbfs(p, True)).selectExpr(
            f"'{dataset}' AS dataset",
            "ListEntry",
            "Name",
            "geometry",
        ),
        allowMissingColumns=False,
    )

sdf_historical_sites = (
    sdf_historical_sites.withColumn("geometry", F.expr("ST_GeomFromWKB(geometry)"))
    .transform(st_clean)
    .withColumn("geometry", F.expr("EXPLODE(ST_Dump(geometry))"))
)

(sdf_historical_sites.withColumn("geometry", F.expr("ST_AsBinary(geometry)")).write.mode("overwrite").parquet(dbfs(f_output_historic_combined, True)))

# COMMAND ----------

sdf_historical_sites.groupBy("dataset").agg(F.count("ListEntry")).display()

# COMMAND ----------

(sdf_historical_sites.withColumn("gtype", F.expr("ST_GeometryType(geometry)")).groupby("gtype").agg(F.count("geometry"))).display()
