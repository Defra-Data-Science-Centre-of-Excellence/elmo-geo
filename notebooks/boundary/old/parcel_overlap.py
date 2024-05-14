# Databricks notebook source
# MAGIC %pip install -q git+https://github.com/aw-west-defra/cdap_geo.git

# COMMAND ----------

import sedona
from pyspark.sql import functions as F

sedona.register.SedonaRegistrator.registerAll(spark)

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from cdap_geo import area, st_join, to_gdf

# COMMAND ----------

f = "dbfs:/mnt/lab/unrestricted/geoparquet/rural_payments_agency/reference_parcels/2021_03_16.parquet"
df_parcels = (
    spark.read.parquet(f)
    .select(
        F.concat("SHEET_ID", "PARCEL_ID").alias("id_parcel"),
        "geometry",
    )
    .withColumn("area", area("geometry"))
)
print(df_parcels.count())
display(df_parcels)

# COMMAND ----------

df = (
    st_join(
        df_parcels.select(F.col("id_parcel").alias("id_left"), "area", "geometry"),
        df_parcels.select(F.col("id_parcel").alias("id_right"), "geometry"),
        from_wkb=True,
    )
    .filter(F.col("id_left") != F.col("id_right"))
    .withColumn("area_overlap", area(intersection("geometry_left", "geometry_right")))
    .filter("0 < area_overlap")
    .withColumn("prop", F.col("area_overlap") / F.col("area"))
    .select(
        "id_left",
        "id_right",
        "area",
        "area_overlap",
        "prop",
    )
)

print(df.count())
display(df)

# COMMAND ----------

f = "dbfs:/mnt/lab/unrestricted/DSMT/gis/parcel_overlap/2022-11-14.parquet"
# df.write.parquet(f)
f = f.replace("dbfs:/", "/dbfs/")
df = pd.read_parquet(f)

df

# COMMAND ----------

df2 = df.groupby(["id_left"]).agg({"area": "first", "area_overlap": "sum", "prop": "sum"}).sort_values("prop", ascending=False).reset_index()

df2

# COMMAND ----------

a, b = df["area_overlap"].sum(), df_parcels.agg(F.sum("area")).collect()[0][0]

f"Overlap={a/10_000:,.0f}ha  England={b/10_000:,.0f}ha  Percent={a/b:%}"

# COMMAND ----------

ids0 = df.query(".01<prop").pipe(lambda df: set(df["id_left"]).union(df["id_right"]))
ids1 = df.query(".99<prop").pipe(lambda df: set(df["id_left"]).union(df["id_right"]))

(
    df2.query("1e-5<prop")["id_left"].nunique(),
    df2.query(" .01<prop")["id_left"].nunique(),
    df2.query(" .1 <prop")["id_left"].nunique(),
    df2.query(" .5 <prop")["id_left"].nunique(),
    len(ids0),
    len(ids1),
)

# COMMAND ----------

gdf = to_gdf(df_parcels.filter(F.col("id_parcel").isin(ids)), crs=27700).merge(df2[["id_left", "prop"]].rename(columns={"id_left": "id_parcel"}))

f = "/dbfs/mnt/lab/unrestricted/DSMT/gis/parcels_that_overlap/2022-11-15.parquet"
gdf.to_parquet(f)
gdf = gpd.read_parquet(f)

gdf

# COMMAND ----------

import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

i, d = 0, dict()
for g in gdf.geometry:
    j = gdf[gdf.intersects(g)]["id_parcel"]
    # j = gdf.clip(g.bounds)['id_parcel']
    for k, v in d.items():
        if not v.isdisjoint(j):
            d[k] = v.union(j)
            break
    else:
        d[i] = set(j)
        i += 1
ids = sorted(d.values(), key=len, reverse=True)

print(sum(len(v) for v in ids), gdf["id_parcel"].nunique())
ids

# COMMAND ----------

f = "/dbfs/mnt/lab/unrestricted/DSMT/LEEP/whole_farm_model/2022_10_25.feather"
wfm = pd.read_feather(f)

wfm

# COMMAND ----------

wfm_gdf = gdf.merge(wfm[["id_business", "id_parcel"]])

wfm_gdf

# COMMAND ----------

import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

d = dict()
for g in wfm_gdf.geometry:
    j = wfm_gdf[wfm_gdf.intersects(g)]["id_parcel"]
    for k, v in d.items():
        if not v.isdisjoint(j):
            d[k] = v.union(j)
            break
    else:
        d[k + 1] = set(j)
wfm_ids = sorted(d.values(), key=len, reverse=True)

print(sum(len(v) for v in wfm_ids), wfm_gdf["id_parcel"].nunique())
wfm_ids = [j for j in wfm_ids if len(j) > 1]

wfm_ids

# COMMAND ----------

[j for i, j in enumerate(wfm_ids) if i in x]

# COMMAND ----------

# x = [1, 3, 4, 10, 12, 27, 36, 43, 44, 45, 53, 73, 80, 90, 116, 118, 123, 147, 148, 151, 153, 167, 170, 171, 178, 186, 192, 197, 202, 203, 215, 221, 237, 246, 254]  # ids
x = [2, 4, 5, 14, 16, 17, 19, 20, 22]  # wfm_ids

fig, axs = plt.subplots(3, 3, figsize=(16, 9))
axs = [ax for axx in axs for ax in axx]
for i, ax in zip(x, axs):
    if i == 22:
        b = 52916
    else:
        b = 67612
    s = wfm_ids[i]
    df = gdf[gdf["id_parcel"].isin(s)]
    ax = df.plot(ax=ax, alpha=0.3, edgecolor="k")
    ax.set(title=f"{b}, {s}")
    ax.axis("off")


# COMMAND ----------

wfm2 = wfm[wfm["id_parcel"].isin(j for i in x for j in wfm_ids[i])]

wfm2

# COMMAND ----------

import geopandas as gpd

gpd.datasets.available


def get_example(name):
    return gpd.read_file(gpd.datasets.get_path(name)).to_crs("WGS84")


naturalearth_lowres = get_example("naturalearth_lowres")
naturalearth_cities = get_example("naturalearth_cities")
nybb = get_example("nybb")

# COMMAND ----------

df_left = naturalearth_cities.copy().rename(columns={"name": "city"})[["city", "geometry"]]

df_right = naturalearth_lowres.copy().rename(columns={"name": "country"})[["country", "geometry"]].buffer(1)

df_left.plot(ax=df_right.plot(color="C0"), color="C1")

# COMMAND ----------

from cdap_geo import geohash
from pandas import concat, merge


def geohash_join(left, right):
    """Join using geohash indexing, with decreasing resolution for a minimum of 1 join."""
    N = min(
        left["geohash"].str.len().max(),
        right["geohash"].str.len().max(),
    )
    for i in range(N, 0, -1):
        left[".geohash"] = left["geohash"].str.slice(stop=i)
        right[".geohash"] = right["geohash"].str.slice(stop=i)
        tmp = merge(left, right, on=".geohash")
        left = left[~left["index"].isin(tmp["index"])]
        yield tmp


def knn_join(left, right, k=1):
    left = left.copy().reset_index().pipe(geohash)
    right = right.copy().pipe(geohash)

    df = (
        concat(geohash_join(left, right))
        .assign(distance=lambda df: df["geometry_x"].distance(df["geometry_y"]))
        .groupby("index")
        .sort_values("distance")
        .head(1)
    )
    return df


df = knn_join(df_left, df_right)
df

# COMMAND ----------

df.sort_values(["index", "distance"]).groupby("index").head(1)

# COMMAND ----------

import pandas as pd

f = "/dbfs/mnt/lab/unrestricted/DSMT/LEEP/whole_farm_model/2022_10_25.feather"
df = pd.read_feather(f)
df
