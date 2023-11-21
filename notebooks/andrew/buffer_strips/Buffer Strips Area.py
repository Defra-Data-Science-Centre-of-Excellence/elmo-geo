# Databricks notebook source
# MAGIC %md
# MAGIC # Buffer Strips
# MAGIC Compare paying per 100m, parcel, or ha.
# MAGIC [Hedgerow Standard](https://townsendcharteredsurveyors.co.uk/sustainable-farming-incentive-pilot-starting-2021-hedgerows-standard/)
# MAGIC [Waterbody Standard](https://townsendcharteredsurveyors.co.uk/sustainable-farming-incentive-pilot-starting-2021-water-body-buffering-standard/)
# MAGIC [Sharepoint Document](https://defra.sharepoint.com/:w:/r/teams/Team1645/Evidence%20and%20Analysis%20WS/4.9_Workstream_Areas_Modelling_Strategy/4.9.3.5_Projects_MS5_Payment_rates/Buffering%20Payment%20Strategy%20Analysis.docx?d=w078dc8ee5888496c924a3e84be78ff55&csf=1&web=1&e=RHbHab)
# MAGIC
# MAGIC Buffer margins and find overlap area.
# MAGIC Hedgerow widths: 4, 6, 8, 10, 12
# MAGIC
# MAGIC ### Data
# MAGIC - LEEP, WFM
# MAGIC   `/dbfs/mnt/lab/unrestricted/DSMT/LEEP/whole_farm_model/2022_07_27.feather`
# MAGIC - RPA, Ecological Focus Area (EFA) Control Layer (Hedge)
# MAGIC   `dbfs:/mnt/lab/unrestricted/elm_data/rural_payments_agency/efa_hedges/2022_06_24.parquet`
# MAGIC - RPA, Reference Parcels
# MAGIC   `dbfs:/mnt/lab/unrestricted/elm_data/rural_payments_agency/reference_parcels/2021_03_16.parquet`
# MAGIC - OS, Watercourse Link
# MAGIC   `dbfs:/mnt/lab/unrestricted/elm_data/ordnance_survey/rivers_canals_streams_vector-WatercourseLink/2021_03_16.parquet`
# MAGIC   *linear features, waiting for OS Master Map*
# MAGIC - OSM, within:England tag:water
# MAGIC   [notebook](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/3068502344869245/)
# MAGIC   `dbfs:/mnt/lab/unrestricted/elm_data/open_street_map/england_water/2023-01-17.parquet`
# MAGIC   *union with OS data to add width to wider waterbodies*
# MAGIC
# MAGIC ### Output data
# MAGIC - Buffer Strips, Geometries
# MAGIC   `dbfs:/mnt/lab/unrestricted/DSMT/gis/buffer_strips_geometries/2023-01-11.parquet`
# MAGIC   `id_business, id_parcel, geometry_parcel, geometry_hedge, geometry_water`
# MAGIC - Buffer Strips, Metrics
# MAGIC   `dbfs:/mnt/lab/unrestricted/DSMT/gis/buffer_strips/2023-01-11.parquet`
# MAGIC   `id_business, id_parcel, ha_parcel, m_hedge, m_water, ...` and other metrics

# COMMAND ----------

# MAGIC %pip install -qU git+https://github.com/aw-west-defra/cdap_geo.git

# COMMAND ----------

# DBTITLE 1,Import
from datetime import datetime

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from cdap_geo.sedona import st, st_join, st_load, st_register
from pyspark.sql import functions as F
from pyspark.sql import types as T

st_register()


today = datetime.now().isoformat().split("T")[0]


def wkbs(data, crs):
    return gpd.GeoSeries.from_wkb(data, crs=crs)


def buf_area(left: str, buf: int, right: str = "geometry_parcel"):
    #   return F.expr(f'ST_Area(ST_Intersection(ST_Buffer({left}, {buf}), {right}))')
    return F.expr(
        f"""ST_Area(
    ST_MakeValid(ST_Intersection(
      ST_MakeValid(ST_Buffer(
        ST_MakeValid(ST_Buffer(
          ST_MakeValid({left}),
          0.001
        )),
        {buf}-0.001
      )),
      ST_MakeValid({right})
    ))
  )"""
    )


# COMMAND ----------

# DBTITLE 1,Filepaths
f_wfm = "/dbfs/mnt/lab/unrestricted/DSMT/LEEP/whole_farm_model/2022_07_27.feather"
sf_hedge = (
    "dbfs:/mnt/lab/unrestricted/geoparquet/rural_payments_agency/efa_hedges/2022_06_24.parquet"
)
sf_parcel = "dbfs:/mnt/lab/unrestricted/geoparquet/rural_payments_agency/reference_parcels/2021_03_16.parquet"
sf_os = "dbfs:/mnt/lab/unrestricted/geoparquet/ordnance_survey/rivers_canals_streams_vector-WatercourseLink/2021_03_16.parquet"
sf_osm = "dbfs:/mnt/lab/unrestricted/geoparquet/osm/england_water/2023-01-17.parquet"

sf_geo = f"dbfs:/mnt/lab/unrestricted/DSMT/gis/buffer_strips_geometries/{today}.parquet"
sf = f"dbfs:/mnt/lab/unrestricted/DSMT/gis/buffer_strips/{today}.parquet"

# COMMAND ----------

# DBTITLE 1,Join Geometries
sdf_wfm = (
    pd.read_feather(f_wfm)[["id_business", "id_parcel"]]
    .pipe(spark.createDataFrame)
    .repartition(200, "id_parcel")
)


sdf_parcel = (
    spark.read.parquet(sf_parcel)
    .select(
        F.concat("SHEET_ID", "PARCEL_ID").alias("id_parcel"),
        st_load("geometry").alias("geometry"),
    )
    .repartition(200, "id_parcel")
)


sdf_hedge = (
    spark.read.parquet(sf_hedge)
    .select(
        F.concat("REF_PARCEL_SHEET_ID", "REF_PARCEL_PARCEL_ID").alias("id_parcel"),
        st_load("geometry").alias("geometry"),
    )
    .groupBy("id_parcel")
    .agg(F.expr(f"ST_Union_Aggr(geometry)").alias("geometry"))
    .repartition(200, "id_parcel")
)


WATERBODY_BUFFER_CROSS_COMPLIANCE = 2
WATERBODY_BUFFER_LINK = 10
sdf_water = (
    spark.read.parquet(sf_os)
    .select("geometry")
    .union(spark.read.parquet(sf_osm).select("geometry"))
    .select(
        st_load("geometry").alias("geometry"),
    )
    .withColumn(
        "geometry",
        F.expr(f"ST_MakeValid(ST_Buffer(geometry, {WATERBODY_BUFFER_CROSS_COMPLIANCE}))"),
    )
    .transform(
        lambda right: st_join(
            sdf_parcel.withColumn(
                "geometry", F.expr(f"ST_Buffer(geometry, {WATERBODY_BUFFER_LINK})")
            ),
            right,
            rsuffix="",
        )
    )
    .groupBy("id_parcel")
    .agg(
        F.expr(f"ST_Union_Aggr(geometry)").alias("geometry"),
        F.first("geometry_left").alias("geometry_left"),
    )
    .withColumn(
        "geometry",
        F.expr("ST_MakeValid(ST_Intersection(ST_MakeValid(ST_Boundary(geometry)), geometry_left))"),
    )
    .drop("geometry_left")
    .repartition(200, "id_parcel")
)


sdf_geo = (
    sdf_wfm.join(
        sdf_parcel.withColumnRenamed("geometry", "geometry_parcel"),
        on="id_parcel",
        how="outer",
    )
    .join(
        sdf_hedge.withColumnRenamed("geometry", "geometry_hedge"),
        on="id_parcel",
        how="left",
    )
    .join(
        sdf_water.withColumnRenamed("geometry", "geometry_water"),
        on="id_parcel",
        how="left",
    )
    .select(
        "id_business",
        "id_parcel",
        "geometry_parcel",
        "geometry_hedge",
        "geometry_water",
    )
    .repartition(200, "id_parcel")
)


sdf_geo.write.parquet(sf_geo, mode="overwrite")
display(sdf_geo)

# COMMAND ----------

# DBTITLE 1,Plot

def sql_startswiths(col: str, starters: list):
    return " OR ".join(f'STARTSWITH({col}, "{s}")' for s in starters)


df = (
    spark.read.parquet(sf_geo)
    .filter(sql_startswiths("id_parcel", ["NY9170", "NY9171", "NY9270", "NY9271"]))
    .select(
        "id_business",
        "id_parcel",
        F.expr("ST_AsBinary(geometry_parcel)").alias("geometry_parcel"),
        F.expr("ST_AsBinary(geometry_hedge)").alias("geometry_hedge"),
        F.expr("ST_AsBinary(geometry_water)").alias("geometry_water"),
    )
    .toPandas()
    .assign(
        geometry_parcel=lambda df: wkbs(df["geometry_parcel"], crs=27700),
        geometry_hedge=lambda df: wkbs(df["geometry_hedge"], crs=27700),
        geometry_water=lambda df: wkbs(df["geometry_water"], crs=27700),
    )
    .pipe(gpd.GeoDataFrame)
)


fig, ax = plt.subplots(1, figsize=(9, 9))
df["geometry_parcel"].plot(ax=ax, color="darkgoldenrod", alpha=0.6, edgecolor="darkgoldenrod").set(
    title="BNG: NY9170-NY9271"
)
df["geometry_hedge"].plot(ax=ax, color="C2")
df["geometry_water"].plot(ax=ax, color="C0")
ax.axis("off")
fig.tight_layout()

# COMMAND ----------

# DBTITLE 1,Calculate Metrics
df = (
    spark.read.parquet(sf_geo)
    .withColumn("ha_parcel", F.expr("ST_Area(geometry_parcel)") / 1e4)
    .withColumn("m_hedge", F.expr("ST_Length(geometry_hedge)"))
    # .withColumn('m_water', F.expr('ST_Length(geometry_water)'))
    .withColumn("ha_hedge_4m", buf_area("geometry_hedge", 4) / 1e4)
    .withColumn("ha_hedge_6m", buf_area("geometry_hedge", 6) / 1e4)
    .withColumn("ha_hedge_8m", buf_area("geometry_hedge", 8) / 1e4)
    .withColumn("ha_hedge_10m", buf_area("geometry_hedge", 10) / 1e4)
    .withColumn("ha_hedge_12m", buf_area("geometry_hedge", 12) / 1e4)
    .withColumn("ha_water_4m", buf_area("geometry_water", 4) / 1e4)
    .withColumn("ha_water_6m", buf_area("geometry_water", 6) / 1e4)
    .withColumn("ha_water_8m", buf_area("geometry_water", 8) / 1e4)
    .withColumn("ha_water_10m", buf_area("geometry_water", 10) / 1e4)
    .withColumn("ha_water_12m", buf_area("geometry_water", 12) / 1e4)
    .drop("geometry_parcel", "geometry_hedge", "geometry_water")
)


df.write.parquet(sf, mode="overwrite")
display(df)

# COMMAND ----------

# DBTITLE 1,Download
display(spark.read.parquet(sf))

# COMMAND ----------

# DBTITLE 1,Plot dodgy buffers
import matplotlib.pyplot as plt
from cdap_geo import to_gdf

df = spark.read.parquet(sf_geo).filter(
    F.col("id_parcel").isin(["SE12659143", "TL73394704", "SE92216981", "ST18041438"])
)

df_parcel = to_gdf(
    df.select(
        "id_parcel",
        F.expr("ST_AsBinary(geometry_parcel)").alias("geometry"),
    )
)

df_hedge = to_gdf(
    df.select(
        "id_parcel",
        F.expr("ST_AsBinary(geometry_hedge)").alias("geometry"),
    )
)

df_buf4 = to_gdf(
    df.select(
        "id_parcel",
        F.expr("ST_AsBinary(ST_Buffer(geometry_hedge, 2))").alias("geometry"),
    )
)

df_buf12 = to_gdf(
    df.select(
        "id_parcel",
        F.expr("ST_AsBinary(ST_Buffer(geometry_hedge, 8))").alias("geometry"),
    )
)

fig, axs = plt.subplots(1, 4, figsize=(16, 9))
for i, ax in enumerate(axs):
    df_parcel[i : i + 1].plot(ax=ax, color="C1", alpha=0.5)
    df_buf4[i : i + 1].plot(ax=ax, color="C4", alpha=0.25)
    df_buf12[i : i + 1].plot(ax=ax, color="C0", alpha=0.25)
    df_hedge[i : i + 1].plot(ax=ax, color="C2")
