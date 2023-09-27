# Databricks notebook source
# MAGIC %md
# MAGIC # Hedgerows Overlap
# MAGIC EFA Hedge data can contain double reporting of hedges that split parcels, if they are reported and maintained by both parties separately.
# MAGIC These are called "Double Counted" hedgerows.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install -qU git+https://github.com/aw-west-defra/cdap_geo.git pyspark_dist_explore

# COMMAND ----------

from pprint import pprint

import matplotlib.pyplot as plt
import pandas as pd
from cdap_geo.sedona import F, st_join, st_load, st_register, st_valid
from pyspark_dist_explore import hist

st_register()

# COMMAND ----------

f = "dbfs:/mnt/lab/unrestricted/elm_data/rural_payments_agency/efa_hedges/2022_06_24.parquet"
f_gis = "dbfs:/mnt/lab/unrestricted/DSMT/gis/hedgerow_overlap_geometries/2023-02-14.parquet"
f_out = "dbfs:/mnt/lab/unrestricted/DSMT/gis/hedgerow_overlap/2023-02-14.parquet"

# COMMAND ----------

spark.read.parquet(f).display()

# COMMAND ----------

df = spark.read.parquet(f).select(
    F.concat("REF_PARCEL_SHEET_ID", "REF_PARCEL_PARCEL_ID").alias("id_parcel"),
    F.monotonically_increasing_id().alias("id"),
    st_load("geometry").alias("geometry"),
)

display(df)

# COMMAND ----------

df_gis = (
    st_join(df, df, lsuffix="", distance=12)
    .filter("id<id_right")
    .filter("id_parcel!=id_parcel_right")
    .withColumn("geometry_right", F.explode(F.expr("ST_Dump(geometry_right)")))
    .groupBy("id")
    .agg(
        F.collect_set("id_right").alias("ids"),
        F.collect_set("id_parcel_right").alias("ids_parcel"),
        F.expr("ST_SimplifyPreserveTopology(ST_Union_Aggr(geometry_right), 0.001)").alias(
            "geometry_right"
        ),
    )
    .join(df, on="id", how="right")
    .withColumn("geometry_right", st_valid("geometry_right"))
)


df_g.write.parquet(f_gis, mode="overwrite")
display(df_gis)

# COMMAND ----------

spark.read.parquet(f_gis).display()

# COMMAND ----------


def fn_overlap(buf):
    return F.expr(
        f"""ST_Length(
    ST_MakeValid(ST_Intersection(
      geometry,
      ST_MakeValid(ST_Buffer(geometry_right, {buf}-0.001))
    ))
  )"""
    ).alias(f"length_overlap{buf}")


df_out = (
    spark.read.parquet(f_gis)
    .withColumn("geometry_right", F.expr("ST_MakeValid(ST_Buffer(geometry_right, 0.001))"))
    .select(
        "id",
        "id_parcel",
        F.expr("ST_Length(geometry)").alias("length"),
        fn_overlap(12),
        fn_overlap(10),
        fn_overlap(8),
        fn_overlap(6),
        fn_overlap(4),
        fn_overlap(0.75),
    )
)

df_out.write.parquet(f_out, mode="overwrite")
display(df_out)

# COMMAND ----------

pdf = df_out.toPandas()
pdf

# COMMAND ----------

target = 572_670_000

pdf["double_counted0.75"] = 0.95 < (pdf["length_overlap0.75"] / pdf["length"])
pdf["double_counted4"] = 0.95 < (pdf["length_overlap4"] / pdf["length"])
pdf["double_counted12"] = 0.95 < (pdf["length_overlap12"] / pdf["length"])

dm1 = (pdf["length"] - pdf["length"] * pdf["double_counted0.75"] / 2).sum()
dm4 = (pdf["length"] - pdf["length"] * pdf["double_counted4"] / 2).sum()
dm12 = (pdf["length"] - pdf["length"] * pdf["double_counted12"] / 2).sum()
pd.DataFrame(
    {
        "index": ["0.75m buffer", "4m buffer", "12m buffer"],
        "1984 hedgerows": [f"{target:,.0f} m", "-", "-"],
        "efa hedgerows": [f'{pdf["length"].sum():,.0f} m', "-", "-"],
        "double counted": [
            f'{pdf["double_counted0.75"].mean():.1%}',
            f'{pdf["double_counted4"].mean():.1%}',
            f'{pdf["double_counted12"].mean():.1%}',
        ],
        "adj hedgerows": [f"{dm1:,.0f} m", f"{dm4:,.0f} m", f"{dm12:,.0f} m"],
        "110%(1984)-2022": [
            f"{1.1*target-dm1:,.0f} m",
            f"{1.1*target-dm4:,.0f} m",
            f"{1.1*target-dm12:,.0f} m",
        ],
        "2022/1984": [f"{dm1/target:.1%}", f"{dm4/target:.1%}", f"{dm12/target:.1%}"],
    }
).set_index("index").T

# COMMAND ----------

fig, (ax0, ax1) = plt.subplots(2, figsize=[8, 4.5], tight_layout=True)
(pdf["length"].hist(ax=ax0, bins=20).set(title="Hedgrow Lengths"))
(
    (pdf["length_overlap0.75"] / pdf["length"])
    .hist(ax=ax1, bins=20)
    .set(title="Single-Sidedness (at 0.75m)")
)
pass
