# Databricks notebook source
# MAGIC %pip install -q git+https://github.com/aw-west-defra/cdap_geo.git

# COMMAND ----------

import pandas as pd
from cdap_geo import (
    buffer,
    pointify,
    st_join,
    st_register,
    unary_union,
    write_geoparquet,
)
from pyspark.sql import functions as F

st_register()

# COMMAND ----------

# DBTITLE 1,Input Data
f = "dbfs:/mnt/lab/unrestricted/geoparquet/natural_england/living_england/2022_09_16.parquet/"
df_living_england = spark.read.parquet(f)
print(df_living_england.count())

f = "dbfs:/mnt/lab/unrestricted/geoparquet/defra/ancient_woodland_inventory/2021_03_12.parquet/"
df_ancient_woodland = spark.read.parquet(f)
print(df_ancient_woodland.count())

f = "dbfs:/mnt/lab/unrestricted/geoparquet/defra/traditional_orchards/2021_03_22.parquet/"
df_traditional_orchards = spark.read.parquet(f)
print(df_traditional_orchards.count())

f = "dbfs:/mnt/lab/unrestricted/geoparquet/englands_community_forests_partnership/community_forests/2021_11_02.parquet/"
df_community_forests = spark.read.parquet(f)
print(df_community_forests.count())

f = "dbfs:/mnt/lab/unrestricted/geoparquet/forestry_commission/felling_licences/2021_03_18.parquet"
df_felling_licences = spark.read.parquet(f)
print(df_felling_licences.count())

f = "dbfs:/mnt/lab/unrestricted/geoparquet/forestry_commission/national_forest_inventory_woodland_england_2018/2021_03_24.parquet"
df_national_forest_inventory = spark.read.parquet(f)
print(df_national_forest_inventory.count())


f = "dbfs:/mnt/lab/unrestricted/geoparquet/rural_payments_agency/reference_parcels/2021_03_16.parquet"
df_parcels = spark.read.parquet(f)
print(df_parcels.count())

f = "dbfs:/mnt/lab/unrestricted/DSMT/LEEP/whole_farm_model/2022_10_25.parquet"
df_wfm = spark.read.parquet(f)
print(df_wfm.count())

# COMMAND ----------

# DBTITLE 1,Pointify Living England
f = "dbfs:/mnt/lab/unrestricted/geoparquet/natural_england/living_england/2022_09_16.parquet/"
df_living_england = spark.read.parquet(f)
print(df_living_england.count())


resolution = 10  # 10m
area = 0.01  # 10mx10m = 0.01ha

woodland = ["Broadleaved, Mixed and Yew Woodland", "Coniferous Woodland"]

A = F.col("A_pred").isin(woodland)
B = F.col("B_pred").isin(woodland)

df_living_england_woodland_points = (
    df_living_england.filter(A | B)
    .withColumn("A_prob", F.when(F.col("A_prob") == 0, 100).otherwise(F.col("A_prob")))
    .withColumn(
        "prob",
        F.when(A, F.col("A_prob"))
        .when(B, F.col("B_prob"))
        .when(A & B, F.col("A_prob") + F.col("B_prob"))
        .otherwise(0)
        / 100,
    )
    .withColumn("area", F.lit(area))
    .select("prob", "area", "geometry")
    .repartition(200)
    .withColumn("geometry", F.explode(pointify("geometry", resolution, False)))
)


f = "dbfs:/mnt/lab/unrestricted/geoparquet/natural_england/living_england_woodland_points/2022_10_10.parquet/"
write_geoparquet(df_living_england_woodland_points, f, mode="overwrite")
print(df_living_england_woodland_points.count())
display(df_living_england_woodland_points)

# COMMAND ----------

# DBTITLE 1,Select Columns
# Living England
df_living_england_woodland_points = spark.read.parquet(
    "dbfs:/mnt/lab/unrestricted/geoparquet/natural_england/living_england_woodland_points/2022_10_10.parquet/"
)
le = df_living_england_woodland_points.select("area", "geometry")


# Ancient Woodland
aw = df_ancient_woodland.select("geometry")


# Traditional Orchards
to = df_traditional_orchards.select("geometry")


# Community Forests
cf = df_community_forests.select("geometry")


# Felling Licences
fl = df_felling_licences.filter(
    F.col("Descriptr").isin(
        [
            "Sel Fell/Thin (Conditional)",
            "Clear Fell (Conditional)",
            # 'Refused FLA',
            # 'Single Tree',
            "Clear Fell (Unconditional)",
            "Sel Fell/Thin (Unconditional)",
        ]
    )
).select("geometry")


# National Forest Inventory
nfi = df_national_forest_inventory.filter(
    (F.col("COUNTRY") == F.lit("England")) & (F.col("CATEGORY") == F.lit("Woodland"))
).select(
    F.col("IFT_IOA").alias("nfi_cat"),
    F.col("Area_ha").alias("nfi_area"),
    "geometry",
)


# Parcels
parcels = df_wfm.select(
    "id_business",
    "id_parcel",
).join(
    df_parcels.select(
        F.concat("SHEET_ID", "PARCEL_ID").alias("id_parcel"),
        "geometry",
    ),
    on="id_parcel",
)

# COMMAND ----------

df0 = le.withColumn("geometry", buffer("geometry", 5, cap_style=3))
df = (
    st_join(parcels, df0, from_wkb=True)
    .groupBy("id_business", "id_parcel")
    .agg(
        F.first("geometry_left").alias("geometry_left"),
        F.collect_list("geometry_right").alias("geometry_right"),
    )
    #   .withColumn('geometry', F.expr('ST_Difference(ST_GeomFromWKB(hex(geometry_left)), ST_Union(ST_GeomFromWKB(hex(geometry_right))))'))
    #   .drop('geometry_left', 'geometry_right')
)

display(df)

# COMMAND ----------

df2 = df.withColumn(
    "geometry",
    F.expr(
        "ST_Difference(ST_GeomFromWKB(hex(geometry_left)), ST_Union(ST_GeomFromWKB(hex(geometry_right))))"
    ),
)
display(df2)

# COMMAND ----------

# Inputs
le
amenity = [aw, to, cf]
commercial = [fl, nfi]
parcels
r = 10 * pd.np.sqrt(pd.np.pi)


# Method
df = parcels

right, name = le, "Living England"

for right, name, sign in zip(
    [le, aw, to, cf, fl, nfi],
    [
        "Living England",
        "Ancient Woodland",
        "Traditional Orchards",
        "Community Forests",
        "Felling Licence",
        "National Forest Inventory",
    ],
    [+1, +1, +1, +1, -1, -1],
):
    if name == "Living England":
        right = right.withColumn("geometry", buffer("geometry", r))
    df = (
        st_join(parcels, right, from_wkb=True, lsuffix="_parcel", rsuffix="")
        .drop("geometry_parcel")
        .withColumn("source", F.lit(name))
        .withColumn("sign", F.lit(sign))
    )
    break


f = "dbfs:/mnt/lab/unrestricted/DSMT/gis/parcel_le/2022_11_21.parquet"


display(df)

# COMMAND ----------


df2 = (
    df.groupBy("id_business", "id_parcel")
    .agg(
        F.first("geometry").alias("geometry"),
        F.collect_list("geometry_right").alias("geometry_right"),
    )
    .withColumn("geometry_right", unary_union("geometry"))
)

display(df2)

# COMMAND ----------

# business, parcel, commercial_woodland_area, amenity_woodland_area

# stats
# distro - business
# distro - parcel
# distro - farm_type, farm_size


# COMMAND ----------

import pandas as pd

df = pd.read_parquet("/dbfs/mnt/lab/unrestricted/DSMT/LEEP/whole_farm_model/2022_10_25.parquet")
df
