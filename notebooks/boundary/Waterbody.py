# Databricks notebook source
# MAGIC %md
# MAGIC # Waterbody v5
# MAGIC OS merge: wtr_fts_water,wtr_fts_waterpoint,wtr_ntwk_waterlink,wtr_ntwk_waterlinkset,wtr_ntwk_waternode
# MAGIC OS waterbody join
# MAGIC OSM waterbody join
# MAGIC
# MAGIC Case study on waterbody for average farm
# MAGIC
# MAGIC ### Statistics
# MAGIC geometry > waterbody in parcel-buffered-at-max(12m)
# MAGIC length boundary by waterbody
# MAGIC length waterbody in parcel
# MAGIC area of waterbody buffer in parcel
# MAGIC 12-4m
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

import elmo_geo

elmo_geo.register(spark)
from math import ceil

from elmo_geo.st import sjoin
from elmo_geo.st.geometry import load_missing


def re_part(sdf):
    batchsize = 10_000
    count = sdf.count()
    parts = ceil(count / batchsize)
    return sdf.repartition(parts)


# COMMAND ----------

sf_parcel = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet"
sf_water_part = "dbfs:/mnt/lab/unrestricted/elm_data/os/ngd/{}.parquet"
datasets = [
    "wtr_fts_water",
    "wtr_fts_waterpoint",
    "wtr_ntwk_waterlink",
    "wtr_ntwk_waterlinkset",
    "wtr_ntwk_waternode",
]

sf_geom = "dbfs:/mnt/lab/unrestricted/elm/elm_se/os_waterbody.parquet"
sf_water = "dbfs:/mnt/lab/unrestricted/elm/elm_se/waterbody.parquet"
sf_drain = "dbfs:/mnt/lab/unrestricted/elm/elm_se/drain.parquet"

# COMMAND ----------

# Merge Water
sdf_os_water = None
for dataset in datasets:
    f_part = sf_water_part.format(dataset)
    sdf_part = spark.read.parquet(f_part).select(
        "osid",
        F.lit(dataset).alias("dataset"),
        "description",
        "geometry",
    )
    sdf_os_water = sdf_os_water.union(sdf_part) if dataset != datasets[0] else sdf_part

print(
    "OS Water:",
    sdf_os_water.rdd.getNumPartitions(),
    sdf_os_water.count(),
    sdf_os_water.filter("geometry IS NULL").count(),
    sdf_os_water.filter("NOT ST_IsValid(geometry)").count(),
)


# Drop ST_Points
sdf_os_water = sdf_os_water.filter('ST_GeometryType(geometry) != "ST_Point"').withColumn("geometry", F.expr("EXPLODE(ST_Dump(geometry))")).transform(re_part)

print(
    "OS Water (no st_points):",
    sdf_os_water.rdd.getNumPartitions(),
    sdf_os_water.count(),
    sdf_os_water.filter("geometry IS NULL").count(),
    sdf_os_water.filter("NOT ST_IsValid(geometry)").count(),
)


# Parcels
sdf_parcel = (
    spark.read.parquet(sf_parcel)
    .transform(re_part)
    .withColumn("geometry", load_missing("geometry"))
    .withColumnRenamed("geometry", "geometry_parcel")
    .withColumn("geometry", F.expr("ST_MakeValid(ST_Buffer(geometry_parcel, 12))"))
)

print(
    "Parcel:",
    sdf_parcel.rdd.getNumPartitions(),
    sdf_parcel.count(),
    sdf_parcel.filter("geometry IS NULL").count(),
    sdf_parcel.filter("NOT ST_IsValid(geometry)").count(),
)


# COMMAND ----------

# Spatially Joined
sdf_geom = (
    sjoin(
        sdf_parcel,
        sdf_os_water,
        lsuffix="_parcel_buffer",
        rsuffix="_water",
        how="left",
    )
    .select(
        "id_business",
        "id_parcel",
        "osid",
        "dataset",
        "description",
        "geometry_parcel",
        "geometry_water",
    )
    .transform(re_part)
)


sdf_geom.write.parquet(sf_geom, mode="overwrite")
print(
    "Spatially Joined:",
    sdf_geom.rdd.getNumPartitions(),
    sdf_geom.count(),
    sdf_geom.select("id_parcel").distinct().count(),
    sdf_geom.filter("geometry_parcel IS NULL or geometry_water IS NULL").count(),
    sdf_geom.filter("NOT ST_IsValid(geometry_parcel) or NOT ST_IsValid(geometry_water)").count(),
)


# COMMAND ----------

display(spark.read.parquet(sf_geom))

# COMMAND ----------

bufs = 4, 6, 8, 10, 12
sdf_geom = spark.read.parquet(sf_geom).withColumn(
    "geometry_water",
    F.expr("ST_MakeValid(ST_Buffer(geometry_water, 0.001))"),
)  # LineStrings into Polygons for ST_Union_Aggr

ha_water = lambda buf: F.expr(
    f"""
  ST_Area(ST_MakeValid(ST_Intersection(
    ST_MakeValid(ST_Buffer(geometry_water, {buf})),
    ST_MakeValid(geometry_parcel)
  )))/10000 AS ha_water_buf{buf}
""",
)  # Area of buffered water in a parcel (ha not sqm)
m_boundary = lambda buf: F.expr(
    f"""
  ST_Length(ST_MakeValid(ST_Intersection(
    ST_MakeValid(ST_Buffer(geometry_water, {buf})),
    ST_MakeValid(ST_Boundary(geometry_parcel))
  ))) AS m_boundary_buf{buf}
""",
)  # Length of parcel boundary in the above area
m_water = (
    lambda buf: F.expr(
        f"""
  ST_Length(ST_MakeValid(ST_Intersection(
    ST_MakeValid(ST_Boundary(geometry_water)),
    ST_MakeValid(ST_Buffer(geometry_parcel, {buf}))
  ))) AS m_water_buf{buf}
""",
    )
)  # Length of water-line beside and inside a parcel (all waterbody geometries are polygons, this may be double for water-lines inside or very close to a parcel)


# COMMAND ----------

sdf_water = (
    sdf_geom.groupby("id_business", "id_parcel")
    .agg(
        F.first("geometry_parcel").alias("geometry_parcel"),
        F.expr("ST_SimplifyPreserveTopology(ST_Union_Aggr(geometry_water), 0) AS geometry_water"),
    )
    .transform(re_part)
    .select(
        "id_business",
        "id_parcel",
        *[ha_water(buf) for buf in bufs],
        *[m_boundary(buf) for buf in bufs],
        *[m_water(buf) for buf in bufs],
    )
)


sdf_water.write.parquet(sf_water, mode="overwrite")
sdf_water.count()

# COMMAND ----------

display(spark.read.parquet(sf_water))

# COMMAND ----------

sdf_drain = (
    sdf_geom.filter('description == "Drain"')
    .groupby("id_business", "id_parcel")
    .agg(
        F.first("geometry_parcel").alias("geometry_parcel"),
        F.expr("ST_SimplifyPreserveTopology(ST_Union_Aggr(geometry_water), 0) AS geometry_water"),
    )
    .transform(re_part)
    .select(
        "id_business",
        "id_parcel",
        *[m_water(buf) for buf in bufs],
        *[m_boundary(buf) for buf in bufs],
        *[ha_water(buf) for buf in bufs],
    )
)


sdf_drain.write.parquet(sf_drain, mode="overwrite")
sdf_drain.count()

# COMMAND ----------

display(spark.read.parquet(sf_drain))
