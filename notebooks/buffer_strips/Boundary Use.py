# Databricks notebook source
# MAGIC %md
# MAGIC # Merge Boundaries
# MAGIC Objective:  assign elibigility to parcel boundary.
# MAGIC   Consider splitting up the boundary for each land use
# MAGIC   Consider recording if the boundary is adjacent to another (and such land use will be shared)
# MAGIC
# MAGIC ### Data
# MAGIC - elm_se, Parcels
# MAGIC - elm_se, Adjacenct Parcels
# MAGIC - elm_se, OS Waterbody
# MAGIC - elm_se, Heritage Wall
# MAGIC - elm_se, Hedgerow
# MAGIC - elmo, WFM Farm Type
# MAGIC - EVAST, Woodland Uptake
# MAGIC - elmo_geo, Priority Habitats
# MAGIC - elmo_geo, Peatland
# MAGIC - elmo_geo, Wetland
# MAGIC
# MAGIC ### Assumptions
# MAGIC **Cross Compliance** is 2m, source: [Aim 2, Intro](https://townsendcharteredsurveyors.co.uk/sustainable-farming-incentive-pilot-starting-2021-water-body-buffering-standard/)
# MAGIC
# MAGIC
# MAGIC ### Outputs
# MAGIC > Table 1 - Neighbouring Land Use
# MAGIC > This table has the nearby geometries for each parcel.  These are geometries joined to parcels with a distance of 12m, they are unioned according to their land use.
# MAGIC > `dbfs:/mnt/lab/unrestricted/elm/elm_se/neighbouring_land_use_geometries.parquet/`
# MAGIC | id_parcel | geometry_parcel | geometry_boundary | geometry_water | geometry_ditch | geometry_wall | geometry_hedge
# MAGIC |---|---|---|---|---|---|---|
# MAGIC |   |   |   |   |   |   |   |
# MAGIC
# MAGIC
# MAGIC > Table 2 - Boundary Land Use - WIP
# MAGIC > This table has the boundary land use.  This table is longer, with multiple id_boundary for each id_parcel.  Each id_boundary has a length and a boolean for each land use.
# MAGIC > `dbfs:/mnt/lab/unrestricted/elm/elm_se/boundary_land_use.parquet/`
# MAGIC | id_business | id_parcel | id_boundary | ha_parcel | geometry_boundary | b_water | b_ditch | b_wall | b_hedge | b_available |
# MAGIC |---|---|---|---|---|---|---|---|---|---|
# MAGIC |   |   |   |   |   |   |   |   |   |   |
# MAGIC
# MAGIC
# MAGIC > Table 3 - Hedgerow Land Use - not started
# MAGIC | farm_type | m/ha_parcel<br>{sum,count,median,mean} | m/ha_water | m/ha_wall | m/ha_hedge | m/ha_available |
# MAGIC | :--------------- |---|---|---|---|---|
# MAGIC | Cereal           |   |   |   |   |   |
# MAGIC | General Cropping |   |   |   |   |   |
# MAGIC | Dairy            |   |   |   |   |   |
# MAGIC | LFA Grazing      |   |   |   |   |   |
# MAGIC | Lowland Grazing  |   |   |   |   |   |
# MAGIC | Mixed            |   |   |   |   |   |
# MAGIC | None             |   |   |   |   |   |
# MAGIC
# MAGIC
# MAGIC > Table 3 - Land Use Change Matrix - not started
# MAGIC |               | Waterbody | Heritage Wall | EVAST | Hedge | Available |
# MAGIC | :------------ |---|---|---|---|---|
# MAGIC | Waterbody     | - |   |   |   |   |
# MAGIC | Heritage Wall |   | - |   |   |   |
# MAGIC | EVAST         |   |   | - |   |   |
# MAGIC | Hedgerow      |   |   |   | - |   |
# MAGIC | Available     |   |   |   |   | - |

# COMMAND ----------

import pandas as pd

import elmo_geo

elmo_geo.register()
from pyspark.sql import functions as F

# COMMAND ----------

f_wfm_farm = "/dbfs/mnt/lab/unrestricted/elm/wfm/2023_06_09/wfm_farms.feather"
sf_parcel = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet/"
sf_os_water = "dbfs:/mnt/lab/unrestricted/elm/elm_se/os_waterbody.parquet/"
sf_wall = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/wall_geometries.parquet/"
sf_hedge = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/hedgerows_geometries.parquet/"

f_evast = "/dbfs/mnt/lab/unrestricted/elm_data/evast/woodland_uptake/2023_07_12.csv"
sf_ph = "dbfs:/mnt/lab/unrestricted/elm/elmo/priority_habitats/output.parquet"
sf_peat = "dbfs:/mnt/lab/unrestricted/elm/elmo/peatland/output.parquet"
sf_wet = "dbfs:/mnt/lab/unrestricted/elm/elmo/ramsar/output.parquet"

sf_adj = "dbfs:/mnt/lab/unrestricted/elm/elm_se/adjacenct_parcel_geometries.parquet/"
sf_neighbour = "dbfs:/mnt/lab/unrestricted/elm/elm_se/neighbouring_land_use_geometries.parquet/"
sf_boundary = "dbfs:/mnt/lab/unrestricted/elm/elm_se/boundary_use_geometries.parquet/"
sf_uptake = "dbfs:/mnt/lab/unrestricted/elm/elm_se/boundary_use_uptake.parquet/"

f_hedge_change = "/dbfs/mnt/lab/unrestricted/elm/elm_se/hedgerow_boundary_use_change.parquet"

# COMMAND ----------

# DBTITLE 1,Parcel Adjacency
sdf_parcel = spark.read.parquet(sf_parcel).select(
    "id_parcel",
    "geometry",
)

buf = 12
sdf_adj = (
    elmo_geo.st.join(
        sdf_parcel,
        sdf_parcel,
        distance=12,
        lsuffix="",
        rsuffix="_right",
    )
    .drop("geometry", "geometry_right")
    .withColumnRenamed("id_parcel_right", "id_parcel_adj")
    .filter("id_parcel != id_parcel_adj")
    .repartition(2000)
)

sdf_adj.write.parquet(sf_adj, mode="overwrite")
display(sdf_adj)
sdf_adj.count()

# COMMAND ----------

# DBTITLE 1,Neighbouring Land Use


def st_union(col):
    return F.expr(
        f"ST_MakeValid(ST_SimplifyPreserveTopology(ST_PrecisionReduce(ST_MakeValid(ST_Union_Aggr({col})), 3), 0)) AS {col}"
    )


def cross_compliance(col, buf):
    return F.expr(f"ST_MakeValid(ST_Buffer({col}, {buf}))")
    # F.expr(f'ST_MakeValid(CASE WHEN ST_GeometryType({col})!="ST_Polygon" THEN {col} ELSE ST_Buffer({col}, {buf}) END)')


sdf_parcel = spark.read.parquet(sf_parcel).select(
    "id_parcel",
    F.expr("ST_MakeValid(ST_Boundary(geometry)) AS geometry_boundary"),
)

sdf_adj = spark.read.parquet(sf_adj).groupby("id_parcel").agg(st_union("geometry_adj"))

sdf_water = (
    spark.read.parquet(sf_os_water)
    .filter('description != "Drain"')
    .withColumn("geometry_water", cross_compliance("geometry_water", 2))
    .groupby("id_parcel")
    .agg(st_union("geometry_water"))
)

sdf_ditch = (
    spark.read.parquet(sf_os_water)
    .filter('description == "Drain"')
    .withColumn("geometry_ditch", cross_compliance("geometry_water", 2))
    .groupby("id_parcel")
    .agg(st_union("geometry_ditch"))
)

sdf_wall = spark.read.parquet(sf_wall).groupby("id_parcel").agg(st_union("geometry_wall"))

sdf_hedge = spark.read.parquet(sf_hedge).select(
    "id_parcel",
    "m_adj",
    "geometry_hedge",
)


sdf_neighbour = (
    sdf_parcel.join(sdf_adj, on="id_parcel", how="left")
    .join(sdf_water, on="id_parcel", how="left")
    .join(sdf_ditch, on="id_parcel", how="left")
    .join(sdf_wall, on="id_parcel", how="left")
    .join(sdf_hedge, on="id_parcel", how="left")
    .select(
        "id_parcel",
        *[
            elmo_geo.io.load_missing(col).alias(col)
            for col in [
                "geometry_boundary",
                "geometry_adj",
                "geometry_water",
                "geometry_ditch",
                "geometry_wall",
                "geometry_hedge",
            ]
        ],
    )
    .repartition(2000)
)


sdf_neighbour.write.parquet(sf_neighbour, mode="overwrite")
display(sdf_neighbour)
sdf_neighbour.count()

# COMMAND ----------

# DBTITLE 1,Boundary Use (geometries)
# Splitting Usage Method


def boundary_use(sdf, use, buf):
    return (
        sdf.withColumn("tmp", F.expr(f"ST_Buffer(geometry_{use}, {buf})"))  # noqa:E128
        .withColumn(
            "tmp",
            F.expr(
                """EXPLODE(Array(
                Array(ST_Intersection(geometry_boundary, tmp), ST_Point(1,1)),
                Array(ST_Difference(geometry_boundary, tmp), ST_Point(0,0))
            ))"""
            ),
        )
        .withColumn("geometry_boundary", F.expr("tmp[0]"))
        .withColumn(f"elg_{use}", F.expr("tmp[1]==ST_Point(1,1)"))
        .drop(f"geometry_{use}", "tmp")
        .filter("NOT ST_IsEmpty(geometry_boundary)")
        .withColumn("geometry_boundary", F.expr("EXPLODE(ST_Dump(geometry_boundary))"))
    )  # noqa:E128


sdf_boundary = (
    spark.read.parquet(sf_neighbour)
    .transform(boundary_use, use="adj", buf=2)
    .transform(boundary_use, use="water", buf=2)  # minus cross compliance
    .transform(boundary_use, use="ditch", buf=2)  # minus cross compliance
    .transform(boundary_use, use="wall", buf=2)
    .transform(boundary_use, use="hedge", buf=2)
    .repartition(2000)
)


sdf_boundary.write.parquet(sf_boundary, mode="overwrite")
display(sdf_boundary)
sdf_boundary.count()

# COMMAND ----------

# DBTITLE 1,Boundary Use (uptake)
sdf_type = (
    pd.read_feather(f_wfm_farm)[["id_business", "farm_type"]]
    .assign(
        farm_type=lambda df: df["farm_type"].map(
            {
                0: None,
                1: "Cereals",
                2: "General Cropping",
                6: "Dairy",
                7: "LFA Grazing",
                8: "Lowland Grazing",
                9: "Mixed",
            }
        )
    )
    .pipe(spark.createDataFrame)
)

sdf_ha = (
    spark.read.parquet(sf_parcel)
    .withColumn("ha", F.expr("ST_Area(geometry)"))
    .select(
        "id_business",
        "id_parcel",
        "ha",
    )
)

sdf_m = (
    spark.read.parquet(sf_boundary)
    .withColumn("m", F.expr("ST_Length(geometry_boundary)"))
    .drop("geometry_boundary")
)


sdf_ph = spark.read.parquet(sf_ph).select(
    "id_parcel",
    F.col("Main_Habit").alias("priority_habitat"),
)

sdf_evast = pd.DataFrame(
    {
        "id_parcel": pd.read_csv(f_evast)["x"],
        "woodland": True,
    }
).pipe(spark.createDataFrame)

sdf_peat = spark.read.parquet(sf_peat).select(
    "id_parcel",
    F.expr("proportion").alias("peatland"),
)

sdf_wet = spark.read.parquet(sf_wet).select(
    "id_parcel",
    F.expr("proportion").alias("wetland"),
)


sdf_uptake = (
    sdf_type.join(sdf_ha, on="id_business", how="full")
    .join(sdf_m, on="id_parcel", how="full")
    .join(sdf_ph, on="id_parcel", how="full")
    .join(sdf_evast, on="id_parcel", how="full")
    .withColumn("woodland", F.expr("COALESCE(woodland, False)"))
    .join(sdf_peat, on="id_parcel", how="full")
    .withColumn("peatland", F.expr("COALESCE(peatland, 0)"))
    .join(sdf_wet, on="id_parcel", how="full")
    .withColumn("wetland", F.expr("COALESCE(wetland, 0)"))
    .select(
        "id_business",
        "id_parcel",
        "farm_type",
        "priority_habitat",
        "elg_adj",
        "elg_water",
        "elg_ditch",
        "elg_wall",
        "elg_hedge",
        "woodland",
        "peatland",
        "wetland",
        "ha",
        "m",
    )
    .repartition(2000)
)


sdf_uptake.write.parquet(sf_uptake, mode="overwrite")
display(sdf_uptake)
sdf_uptake.count()

# COMMAND ----------

sdf_ph.count(), sdf_peat.count(), sdf_wet.count(), sdf_evast.count()

# COMMAND ----------

spark.read.parquet(sf_uptake).select(
    "elg_adj", "elg_water", "elg_ditch", "elg_wall", "elg_hedge", "m"
).display()

# COMMAND ----------

spark.read.parquet(sf_uptake).select(
    F.expr("m").alias("ignoring_adjacency"),
    F.expr("m * (1 - .5 * CAST(elg_adj AS DOUBLE))").alias("boundary"),
    F.expr("boundary * CAST(elg_water AS DOUBLE)").alias("water"),
    F.expr("boundary * CAST(elg_ditch AS DOUBLE)").alias("ditch"),
    F.expr("boundary * CAST(elg_wall AS DOUBLE)").alias("wall"),
    F.expr("boundary * CAST(elg_hedge AS DOUBLE)").alias("hedge"),
    F.expr(
        "boundary * CAST(NOT (elg_water OR elg_ditch OR elg_wall OR elg_hedge) AS DOUBLE)"
    ).alias("available"),
    F.expr(
        "boundary * CAST(elg_hedge AND NOT (elg_water OR elg_ditch OR elg_wall) AS DOUBLE)"
    ).alias("hedge_only"),
    F.expr("boundary * CAST(elg_hedge AND woodland AS DOUBLE)").alias("hedge_on_ewco"),
    F.expr("boundary * CAST(elg_ditch AND .1<peatland AS DOUBLE)").alias("ditch_on_peatland"),
).groupby().sum().display()

# COMMAND ----------

# DBTITLE 1,Example: Hedgerow Boundary Use
df = (
    spark.read.parquet(sf_uptake)
    .withColumnRenamed("m", "m_unadj")
    .withColumn("m", F.expr("m_unadj * (1-0.5*CAST(elg_adj AS DOUBLE))"))
    .groupby("id_parcel", "farm_type")
    .agg(
        # SUM Meterage
        F.expr("SUM(m_unadj) AS m_boundary_unadj"),
        F.expr("SUM(m) AS m_boundary"),
        # F.expr('SUM(m*CAST(elg_water AS DOUBLE)) AS m_water'),
        # F.expr('SUM(m*CAST(elg_ditch AS DOUBLE)) AS m_ditch'),
        # F.expr('SUM(m*CAST(elg_wall AS DOUBLE)) AS m_wall'),
        F.expr("SUM(m*CAST(elg_hedge AS DOUBLE)) AS m_hedge"),
        F.expr("SUM(m*CAST(woodland AS DOUBLE)) AS m_evast"),
        F.expr(
            "SUM(m*CAST(NOT (elg_water OR elg_ditch OR elg_wall OR elg_hedge) AS DOUBLE)) AS m_available"
        ),
        F.expr(
            "SUM(m*CAST(elg_hedge AND (NOT (elg_water OR elg_ditch OR elg_wall OR woodland)) AS DOUBLE)) AS m_hedge_only"
        ),
        F.expr("SUM(m*CAST(elg_water AND woodland AS DOUBLE)) AS m_hedge_and_evast"),
        # Meters per Hectare
        F.expr("SUM(m_unadj/ha) AS mpha_boundary_unadj"),
        F.expr("SUM(m/ha) AS mpha_boundary"),
        # F.expr('SUM(m/ha*CAST(elg_water AS DOUBLE)) AS mpha_water'),
        # F.expr('SUM(m/ha*CAST(elg_ditch AS DOUBLE)) AS mpha_ditch'),
        # F.expr('SUM(m/ha*CAST(elg_wall AS DOUBLE)) AS mpha_wall'),
        F.expr("SUM(m/ha*CAST(elg_hedge AS DOUBLE)) AS mpha_hedge"),
        F.expr("SUM(m/ha*CAST(woodland AS DOUBLE)) AS mpha_evast"),
        F.expr(
            "SUM(m/ha*CAST(NOT (elg_water OR elg_ditch OR elg_wall OR elg_hedge) AS DOUBLE)) AS mpha_available"
        ),
        F.expr(
            "SUM(m/ha*CAST(elg_hedge AND (NOT (elg_water OR elg_ditch OR elg_wall OR woodland)) AS DOUBLE)) AS mpha_hedge_only"
        ),
        F.expr("SUM(m/ha*CAST(elg_water AND woodland AS DOUBLE)) AS mpha_hedge_and_evast"),
    )
    .toPandas()
    .groupby("farm_type")
    .agg(["sum", "count", "median", "mean"])
    .reset_index()
    # Pivot
    .melt(
        id_vars="farm_type",
        var_name=["tmp", "metric"],
    )
    .assign(
        unit=lambda df: df["tmp"].str.split("_").str[0],
        usage=lambda df: df["tmp"].str.split("_").str[1:].str.join("_"),
    )
    .drop(columns=["tmp"])
    # .drop(columns=[('mpha', 'count'), ('mpha', 'sum')])
    .pivot_table(
        values="value",
        index=["farm_type", "usage"],
        columns=["unit", "metric"],
    )
    .sort_index()
    # End Pivot
)


df.to_parquet(f_hedge_change)
pd.options.display.float_format = "{:,.3f}".format
df
