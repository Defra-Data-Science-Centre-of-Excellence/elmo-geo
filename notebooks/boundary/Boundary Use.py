# Databricks notebook source
import pandas as pd
from datetime import datetime as dt

import elmo_geo
from elmo_geo.st import sjoin
from elmo_geo.st.geometry import load_geometry, load_missing
from elmo_geo.io import download_link

elmo_geo.register()
from pyspark.sql import functions as F

# COMMAND ----------

f_wfm_farm = "/dbfs/mnt/lab/unrestricted/elm/wfm/2023_06_09/wfm_farms.feather"
sf_wfm_field = "dbfs:/mnt/lab/restricted/ELM-Project/stg/wfm-field-2024_01_26.parquet"
sf_parcel = "dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet"

sf_os_water = "dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-water-2024_01_26.parquet" # joined to adas parcels
sf_wall = "dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-wall-2024_01_26.parquet" # joined to adas parcels
sf_hedge = "dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-hedge-2024_01_26.parquet" # joined to adas parcels

f_evast = "/dbfs/mnt/lab/unrestricted/elm_data/evast/woodland_uptake/2023_07_12.csv"
sf_ph = "dbfs:/mnt/lab/unrestricted/elm/elmo/priority_habitats/output.parquet"
sf_peat = "dbfs:/mnt/lab/unrestricted/elm/elmo/peatland/output.parquet"
sf_wet = "dbfs:/mnt/lab/unrestricted/elm/elmo/ramsar/output.parquet"

date = "09-04-2024"
sf_adj = f"dbfs:/mnt/lab/restricted/ELM-Project/ods/boundary-use-adjacenct_parcel_geometries-{date}.parquet/"
sf_neighbour = f"dbfs:/mnt/lab/restricted/ELM-Project/ods/boundary-use-neighbouring_land_use_geometries-{date}.parquet/"
sf_boundary = f"dbfs:/mnt/lab/restricted/ELM-Project/ods/boundary-use-geometries-{date}.parquet/"
sf_uptake = f"dbfs:/mnt/lab/restricted/ELM-Project/ods/boundary-use-uptake-{date}.parquet/"
sf_boundary_lengths = f"dbfs:/mnt/lab/restricted/ELM-Project/out/boundary-use-lengths-{date}.parquet"

f_hedge_change = "/dbfs/mnt/lab/unrestricted/elm/elm_se/hedgerow_boundary_use_change.parquet"

# COMMAND ----------

sdf_parcel = spark.read.format("geoparquet").load(sf_parcel)
sdf_parcel.display()

# COMMAND ----------

# DBTITLE 1,Parcel Adjacency
simplify = lambda col: F.expr(f"ST_SimplifyPreserveTopology(ST_Force_2D(ST_MakeValid({col})), 1) AS {col}")

sdf_parcel = (spark.read.format("geoparquet").load(sf_parcel)
              .select(
                  "id_parcel", 
                  "geometry",
              )
              .withColumn("geometry", F.expr("ST_MakeValid(geometry)"))
              .withColumn("geometry", F.expr("ST_Force_2D(geometry)"))
              #.withColumn("geometry", F.expr("ST_PrecisionReduce(geometry, 3)")) # made the next stage fail
              .withColumn("geometry", F.expr("ST_SimplifyPreserveTopology(geometry, 1.0)"))
)

sdf_wfm_field = (spark.read.format("parquet").load(sf_wfm_field)
                 .select(
                     "id_parcel",
                     "id_business",
                 )
                 .dropDuplicates())

sdf_parcel = sdf_parcel.join(sdf_wfm_field, on='id_parcel', how = 'left')

buf = 12
sdf_adj = (
    sjoin(
        sdf_parcel,
        sdf_parcel,
        distance=12,
        lsuffix="",
        rsuffix="_adj",
    )
    .drop("geometry")
    .filter("id_parcel != id_parcel_adj")
    .repartition(2000)
    .withColumn("geometry_adj", F.expr("ST_AsBinary(geometry_adj)"))
)

sdf_adj.write.format("parquet").mode("overwrite").save(sf_adj)
display(sdf_adj)
sdf_adj.count()

# COMMAND ----------

# Report number of parcel intersections with same, different, or unknow business.
# Indicates which situation is most common.
count_null_bid = (spark.read.parquet(sf_adj)
 .filter("(id_business is null) or (id_business_adj is null)")
).count()

count_diff_bid = (spark.read.parquet(sf_adj)
 .filter("id_business != id_business_adj")
).count()

count_same_bid = (spark.read.parquet(sf_adj)
 .filter("id_business = id_business_adj")
).count()

print(f"Number of intersections with same business: {count_same_bid:,.0f}")
print(f"Number of intersections with different business: {count_diff_bid:,.0f}")
print(f"Number of intersections with unknown business: {count_null_bid:,.0f}")

# COMMAND ----------

# DBTITLE 1,Neighbouring Land Use
cross_compliance = lambda col, buf: F.expr(f"ST_MakeValid(ST_Buffer({col}, {buf}))")
st_union = lambda col: F.expr(f"ST_MakeValid(ST_Union_Aggr(ST_SimplifyPreserveTopology(ST_PrecisionReduce(ST_Force_2D(ST_MakeValid({col})), 3), 1))) AS {col}")
boundary = lambda col: F.expr(f"ST_MakeValid(ST_Force_2D(ST_PrecisionReduce(ST_SimplifyPreserveTopology(ST_Boundary({col}), 1), 3))) AS geometry_boundary")


sdf_adj_same = (spark.read.parquet(sf_adj)
           .withColumn("geometry_adj_same_bus", F.expr("ST_GeomFromWKB(geometry_adj)"))
           .filter("(id_business == id_business_adj) or (id_business is null) or (id_business_adj is null)")
           .groupby("id_parcel")
           .agg(st_union("geometry_adj_same_bus"))
)

sdf_adj_diff = (spark.read.parquet(sf_adj)
           .withColumn("geometry_adj_diff_bus", F.expr("ST_GeomFromWKB(geometry_adj)"))
           .filter("id_business != id_business_adj")
           .groupby("id_parcel")
           .agg(st_union("geometry_adj_diff_bus")) # this with line above worked.
)

sdf_adj_comb = sdf_adj_same.join(sdf_adj_diff, on = "id_parcel", how = "outer")


sdf_water = (
    spark.read.format("geoparquet").load(sf_os_water)
    .filter('class != "water-ditch"')
    .withColumn("geometry_water", cross_compliance("geometry", 2))
    .groupby("id_parcel")
    .agg(st_union("geometry_water"))
)

sdf_ditch = (
    spark.read.format("geoparquet").load(sf_os_water)
    .filter('class = "water-ditch"')
    .withColumn("geometry_ditch", cross_compliance("geometry", 2))
    .groupby("id_parcel")
    .agg(st_union("geometry_ditch"))
)

sdf_wall = (spark.read.format("geoparquet").load(sf_wall)
            .filter('class != "wall-relict"') # these are SHINE features and should be excluded
            .withColumn("geometry_wall", cross_compliance("geometry", 2))
            .groupby("id_parcel")
            .agg(st_union("geometry_wall"))
)


sdf_hedge = (spark.read.format("geoparquet").load(sf_hedge)
             .withColumn("geometry_hedge", cross_compliance("geometry", 2))
            .groupby("id_parcel")
            .agg(st_union("geometry_hedge"))
)


sdf_parcel = (spark.read.format("geoparquet").load(sf_parcel)
              .select(
                      "id_parcel",
                      boundary("geometry"),
              )
)

sdf_neighbour = (
    sdf_parcel
    .join(sdf_adj_comb, on="id_parcel", how="left")
    .join(sdf_water, on="id_parcel", how="left")
    .join(sdf_ditch, on="id_parcel", how="left")
    .join(sdf_wall, on="id_parcel", how="left")
    .join(sdf_hedge, on="id_parcel", how="left")
    .select(
        "id_parcel",
        *[
            load_missing(col).alias(col)
            for col in [
                "geometry_boundary",
                "geometry_adj_diff_bus",
                "geometry_adj_same_bus",
                "geometry_water",
                "geometry_ditch",
                "geometry_wall",
                "geometry_hedge",
            ]
        ],
    )
    .select(
        "id_parcel",
        *[
            F.expr(f"ST_AsBinary({col})").alias(col)
            for col in [
                "geometry_boundary",
                "geometry_adj_diff_bus",
                "geometry_adj_same_bus",
                "geometry_water",
                "geometry_ditch",
                "geometry_wall",
                "geometry_hedge",
            ]
        ],
    )
    .repartition(2000)
)

sdf_neighbour.write.format("parquet").mode("overwrite").save(sf_neighbour)

# COMMAND ----------

sdf_neighbour = spark.read.format("parquet").load(sf_neighbour)
display(sdf_neighbour)
sdf_neighbour.count()

# COMMAND ----------

# DBTITLE 1,Boundary Use (geometries)
# Splitting Usage Method
boundary_use = lambda sdf, use, buf: (
    sdf.withColumn("tmp", F.expr(f"ST_Buffer(geometry_{use}, {buf})"))
    .withColumn(
        "tmp",
        F.expr(
            """EXPLODE(Array(
    Array(ST_Intersection(geometry_boundary, tmp), ST_Point(1,1)),
    Array(ST_Difference(geometry_boundary, tmp), ST_Point(0,0))
  ))""",
        ),
    )
    .withColumn("geometry_boundary", F.expr("tmp[0]"))
    .withColumn(f"elg_{use}", F.expr("tmp[1]==ST_Point(1,1)"))
    .drop(f"geometry_{use}", "tmp")
    .filter("NOT ST_IsEmpty(geometry_boundary)")
    .withColumn("geometry_boundary", F.expr("EXPLODE(ST_Dump(geometry_boundary))"))
)


sdf_boundary = (
    spark.read.parquet(sf_neighbour)
    .select(
        "id_parcel",
        *[
            F.expr(f"ST_GeomFromWKB({col})").alias(col)
            for col in [
                "geometry_boundary",
                "geometry_adj_diff_bus",
                "geometry_adj_same_bus",
                "geometry_water",
                "geometry_ditch",
                "geometry_wall",
                "geometry_hedge",
            ]
        ],
    )
    .transform(boundary_use, use="adj_diff_bus", buf=2)
    .transform(boundary_use, use="adj_same_bus", buf=2)
    .transform(boundary_use, use="water", buf=2)  # minus cross compliance
    .transform(boundary_use, use="ditch", buf=2)  # minus cross compliance
    .transform(boundary_use, use="wall", buf=2)
    .transform(boundary_use, use="hedge", buf=2)
    .repartition(2000)
    .withColumn("geometry_boundary", F.expr("ST_AsBinary(geometry_boundary)"))
)


sdf_boundary.write.format("parquet").mode("overwrite").save(sf_boundary)
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
            },
        ),
    )
    .pipe(spark.createDataFrame)
)

# parcel to business lookup
sdf_wfm_field = (spark.read.format("parquet").load(sf_wfm_field)
                 .select(
                     "id_parcel",
                     "id_business",
                 )
                 .dropDuplicates())


sdf_ha = (
    spark.read.format("geoparquet").load(sf_parcel)
    .withColumn("ha", F.expr("ST_Area(geometry)"))
    .select(
        "id_parcel",
        "ha",
    )
)

sdf_m = (spark.read.parquet(sf_boundary)
         .withColumn("geometry_boundary", F.expr("ST_GeomFromWKB(geometry_boundary)"))
         .withColumn("m", F.expr("ST_Length(geometry_boundary)")).drop("geometry_boundary")
)

sdf_ph = spark.read.parquet(sf_ph).select(
    "id_parcel",
    F.col("Main_Habit").alias("priority_habitat"),
)

sdf_evast = pd.DataFrame(
    {
        "id_parcel": pd.read_csv(f_evast)["x"],
        "woodland": True,
    },
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
    sdf_ha
    .join(sdf_wfm_field, on="id_parcel", how="left")
    .join(sdf_type, on = "id_business", how="left")
    .join(sdf_m, on="id_parcel", how="left")
    .join(sdf_ph, on="id_parcel", how="left")
    .join(sdf_evast, on="id_parcel", how="left")
    .withColumn("woodland", F.expr("COALESCE(woodland, False)"))
    .join(sdf_peat, on="id_parcel", how="left")
    .withColumn("peatland", F.expr("COALESCE(peatland, 0)"))
    .join(sdf_wet, on="id_parcel", how="left")
    .withColumn("wetland", F.expr("COALESCE(wetland, 0)"))
    .select(
        "id_business",
        "id_parcel",
        "farm_type",
        "priority_habitat",
        "elg_adj_diff_bus",
        "elg_adj_same_bus",
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

# check no nulls in id_parcel or elg columns
sdf_uptake = spark.read.parquet(sf_uptake)

n_null_parcel_ids = (sdf_uptake.filter("(id_parcel is null)")).count()
n_null_boundary_categories = (sdf_uptake.filter("(id_parcel is not null) and ((elg_adj_diff_bus is null) or (elg_adj_same_bus is null) or (elg_water is null) or (elg_ditch is null) or (elg_wall is null) or (elg_hedge is null))")).count()

if (n_null_parcel_ids == n_null_boundary_categories == 0)==False:
    msg = "Unexpected nulls in uptake dataset"
    raise ValueError(msg)

# COMMAND ----------

sdf_lengths = (spark.read.parquet(sf_uptake)
      .withColumn("boundary_unadj", F.col("m"))
      .withColumn("boundary", F.expr("m * (1 - .5 * CAST( (elg_adj_same_bus OR elg_adj_diff_bus) AS DOUBLE))"))
      .groupBy("id_parcel")
      .agg(
          F.sum("boundary_unadj").alias("m_boundary_unadj"),
          F.sum("boundary").alias("m_boundary"),
          F.sum(F.expr("boundary * CAST(elg_water AS DOUBLE)")).alias("m_water"),
          F.sum(F.expr("boundary * CAST(elg_ditch AS DOUBLE)")).alias("m_ditch"),
          F.sum(F.expr("boundary * CAST(elg_wall AS DOUBLE)")).alias("m_wall"),
          F.sum(F.expr("boundary * CAST(elg_hedge AS DOUBLE)")).alias("m_hedge"),
          F.sum(F.expr("boundary * CAST(NOT (elg_water OR elg_ditch OR elg_wall OR elg_hedge) AS DOUBLE)")).alias("m_available"),
          F.sum(F.expr("boundary * CAST(NOT (elg_water OR elg_ditch OR elg_wall OR elg_hedge OR elg_adj_diff_bus) AS DOUBLE)")).alias("m_available_same_business"),
          F.sum(F.expr("boundary * CAST(elg_hedge AND NOT (elg_water OR elg_ditch OR elg_wall) AS DOUBLE)")).alias("m_hedge_only"),
          F.sum(F.expr("boundary * CAST(elg_hedge AND woodland AS DOUBLE)")).alias("m_hedge_on_ewco"),
          F.sum(F.expr("boundary * CAST(elg_ditch AND .1<peatland AS DOUBLE)")).alias("m_ditch_on_peatland"),
          )
      )
sdf_lengths.write.parquet(sf_boundary_lengths, mode="overwrite")
sdf_lengths.display()

# COMMAND ----------

sdf_lengths = spark.read.parquet(sf_boundary_lengths)
sdf_lengths.groupby().sum().display()

# COMMAND ----------

# download lengths data
pandas_df = spark.read.parquet(sf_boundary_lengths).toPandas()
path_parquet = sf_boundary_lengths.replace("dbfs:", "/dbfs").replace("boundary-use-lengths", "boundary-use-lengths-output")
path_feather = sf_boundary_lengths.replace("dbfs:", "/dbfs").replace("boundary-use-lengths", "boundary-use-lengths-output").replace(".parquet", ".feather")
path_csv = sf_boundary_lengths.replace("dbfs:", "/dbfs").replace("boundary-use-lengths", "boundary-use-lengths-output").replace(".parquet", ".csv")

# output
pandas_df.to_parquet(path_parquet)
pandas_df.to_feather(path_feather)
pandas_df.to_csv(path_csv, index=False)
download_link(path_parquet)
download_link(path_feather)
download_link(path_csv)

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
        F.expr("SUM(m*CAST(NOT (elg_water OR elg_ditch OR elg_wall OR elg_hedge) AS DOUBLE)) AS m_available"),
        F.expr("SUM(m*CAST(elg_hedge AND (NOT (elg_water OR elg_ditch OR elg_wall OR woodland)) AS DOUBLE)) AS m_hedge_only"),
        F.expr("SUM(m*CAST(elg_water AND woodland AS DOUBLE)) AS m_hedge_and_evast"),
        # Meters per Hectare
        F.expr("SUM(m_unadj/ha) AS mpha_boundary_unadj"),
        F.expr("SUM(m/ha) AS mpha_boundary"),
        # F.expr('SUM(m/ha*CAST(elg_water AS DOUBLE)) AS mpha_water'),
        # F.expr('SUM(m/ha*CAST(elg_ditch AS DOUBLE)) AS mpha_ditch'),
        # F.expr('SUM(m/ha*CAST(elg_wall AS DOUBLE)) AS mpha_wall'),
        F.expr("SUM(m/ha*CAST(elg_hedge AS DOUBLE)) AS mpha_hedge"),
        F.expr("SUM(m/ha*CAST(woodland AS DOUBLE)) AS mpha_evast"),
        F.expr("SUM(m/ha*CAST(NOT (elg_water OR elg_ditch OR elg_wall OR elg_hedge) AS DOUBLE)) AS mpha_available"),
        F.expr("SUM(m/ha*CAST(elg_hedge AND (NOT (elg_water OR elg_ditch OR elg_wall OR woodland)) AS DOUBLE)) AS mpha_hedge_only"),
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
