# Databricks notebook source
# MAGIC %md
# MAGIC # Hedgerow and Waterbodies on Parcels
# MAGIC This project identifies potential hedgerow and waterbody datasets.  Spatially joins them to parcels, then calculates some metrics on the length and
# MAGIC areas potentials for buffer strips.
# MAGIC
# MAGIC ### Methodology
# MAGIC **Parcels,** from November 2021 are a requirement for this project, as they align with the parcels used in WFM and by CEH-EVAST.
# MAGIC **Hedgerows,** are sourced from RPA's Environmental Focus Areas, they are linear features attached to parcels.  Except hedgerows from November 2021
# MAGIC are unavailable, so latest hedgerows are used and spatially joined to old parcels.  They are grouped together to create a **single MultiLineString
# MAGIC per parcel**.
# MAGIC **Waterbodies,** are sourced from OS NGD December 2022, a still in development replacement for master map.  All "wtr" features and networks are
# MAGIC included, except catchment areas (wtr_fts_riverbasindistrictcatchment, wtr_fts_waterbodycatchment), these are merged together as a single dataframe
# MAGIC and spatially joined with parcels, they are grouped together creating a **single GeometryCollection per parcel**.  However, to calculate length and
# MAGIC area overlap they geometries need to be of the same type, and such for the metrics the geometry collection is buffered 1mm, to create a single
# MAGIC MultiPolygon per parcel.
# MAGIC **Spatial joins,** are at 12 meters distance, so buffering can be applied to more distant features.
# MAGIC
# MAGIC ### Data Sources
# MAGIC - RPA - Reference Parcels - 2021 November
# MAGIC     *(This version is used by LEEP-WFM)*
# MAGIC - RPA - LF_MANAGED_MV - 2023 December
# MAGIC     *(This is managed hedgerows eligible for agreements, the latest version is used as Nov 2021 was unavailable.)*
# MAGIC - OS - NGD - 2022 December
# MAGIC     *(This contains OS waterbody features, and is was the latest version of NGD ingested.)*
# MAGIC
# MAGIC ### Output Data
# MAGIC - elmo_geo-hedge-2024_01_08.gpkg:  id_parcel, geometry_hedge
# MAGIC - elmo_geo-water-2024_01_08.gpkg:  id_parcel, geometry_water
# MAGIC - elmo_geo-buffers-2024_01_08.csv:  id_parcel, m_hedge, m_water, ha_hedge_buf0m, ha_hedge_buf4m, ha_hedge_buf8m, ha_hedge_buf12m, ha_water_buf0m,
# MAGIC ha_water_buf4m, ha_water_buf8m, ha_water_buf12m
# MAGIC Upload to: [ukceh sharepoint](https://cehacuk.sharepoint.com/sites/EVAST/DEFRA%20Data%20Share/Forms/AllItems.aspx?id=%2Fsites%2FEVAST%2FDEFRA%20Data%20Share%2FIncoming%20Data%2FHedges%20And%20Water%20Body%20data%20Dec&viewid=2ea78def%2D1f62%2D4d01%2D9671%2D188ffdc282c6&OR=Teams%2DHL&CT=1702481423149&clickparams=eyJBcHBOYW1lIjoiVGVhbXMtRGVza3RvcCIsIkFwcFZlcnNpb24iOiI0OS8yMzExMDIzMTgxMCIsIkhhc0ZlZGVyYXRlZFVzZXIiOmZhbHNlfQ)
# MAGIC
# MAGIC ### Output Columns
# MAGIC - id_parcel: RPA reference parcel SHEET_ID + PARCEL_ID from Nov 2021.
# MAGIC
# MAGIC - geometry_parcel: RPA reference parcel geometries from Nov 2021, this version is commonly used within ELM Modelling, EVAST, and ADAS.
# MAGIC - geometry_hedge: RPA managed hedgerow geometries from Dec 2023, spatially joined with a 12 meters distance to 2021 parcels.  These are all linear
# MAGIC features grouped together per parcel, individual geometries are duplicated for each parcel, i.e. 1 MultiLineString per id_parcel.
# MAGIC - geometry_water: OS NGD water features (excluding basins) and networks, spatially joined within 12 meters to 2021 parcels.  Points and linear
# MAGIC features are buffered 1mm for conversion to a polygon.  These features are also grouped together, i.e. 1 MultiPolygon per id_parcel.
# MAGIC
# MAGIC - m_hedge: meterage of hedgerow that are within the parcel geometry.  `st_length(st_intersects(geometry_parcel, geometry_hedge))`
# MAGIC - ha_hedge_buf0m: hectarage of hedgerow within the parcel geometry, this should be zero as hedgerows are linear features.
# MAGIC `st_area(st_intersects(geometry_parcel, geometry_hedge))`
# MAGIC - ha_hedge_buf4m: hectarage of parcels within 4 meters from a hedgerow, meters is ensured by using CRS=BNG/EPSG:27700.
# MAGIC `st_area(st_intersects(geometry_parcel, st_buffer(geometry_hedge, 4)))`
# MAGIC - ha_hedge_buf8m: same as previous but with an 8 meter buffer.
# MAGIC - ha_hedge_buf12m: same as previous but with a 12 meter buffer.
# MAGIC
# MAGIC - m_water: meterage of waterbody that are within the parcel geometry.  `st_length(st_intersects(geometry_parcel, st_boundary(geometry_hedge)))`,
# MAGIC ST_Boundary is added due to the waterbody being a MultiPolygon.
# MAGIC - ha_water_buf0m: hectarage of waterbodies within the parcel geometry.
# MAGIC - ha_water_buf4m: hectarage of parcels within 4 meters from a waterbody.
# MAGIC - ha_water_buf8m: same as previous but with an 8 meter buffer.
# MAGIC - ha_water_buf12m: same as previous but with a 12 meter buffer.
# MAGIC

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

import elmo_geo
from elmo_geo.io import download_link, load_sdf, to_gpq
from elmo_geo.st import sjoin
from elmo_geo.st.udf import st_union

elmo_geo.register()

# COMMAND ----------

# stg2ods
sdf_rpa_parcel = (
    spark.read.format("parquet")
    .load("dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-parcel-adas.parquet")
    .select(
        F.expr("CONCAT(RLR_RW_REFERENCE_PARCELS_DEC_21_SHEET_ID, RLR_RW_REFERENCE_PARCELS_DEC_21_PARCEL_ID) AS id_parcel"),
        F.expr("ST_SetSRID(ST_GeomFromWKB(Shape), 27700) AS geometry"),
    )
    .transform(to_gpq, "dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet")
)

sdf_rpa_parcel_2023 = (
    spark.read.format("parquet")
    .load("dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-parcel-2023_12_13.parquet")
    .select(
        F.expr("CONCAT(SHEET_ID, PARCEL_ID) AS id_parcel"),
        F.expr('TO_TIMESTAMP(VALID_FROM, "yyyyMMddHHmmss") AS valid_from'),
        F.expr('TO_TIMESTAMP(VALID_TO, "yyyyMMddHHmmss") AS valid_to'),
        F.expr("ST_SetSRID(ST_GeomFromWKB(GEOM), 27700) AS geometry"),
    )
    .transform(to_gpq, "dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-2023_12_13.parquet")
)


sdf_rpa_hedge = (
    spark.read.format("parquet")
    .load("dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-hedge-2023_12_13.parquet")
    .select(
        F.expr("CONCAT(REF_PARCEL_SHEET_ID, REF_PARCEL_PARCEL_ID) AS id_parcel"),
        F.expr("ADJACENT_PARCEL_PARCEL_ID IS NOT NULL AS adj"),
        F.expr('TO_TIMESTAMP(VALID_FROM, "yyyyMMddHHmmss") AS valid_from'),
        F.expr('TO_TIMESTAMP(VALID_TO, "yyyyMMddHHmmss") AS valid_to'),
        F.expr("ST_SetSRID(ST_GeomFromWKB(GEOM), 27700) AS geometry"),
    )
    .transform(to_gpq, "dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-hedge-2023_12_13.parquet")
)


os_schema = T.StructType(
    [
        T.StructField("layer", T.StringType(), True),
        T.StructField("theme", T.StringType(), True),
        T.StructField("description", T.StringType(), True),
        T.StructField("width", T.DoubleType(), True),
        T.StructField("geometry", T.BinaryType(), True),
    ]
)

sdf_os_water = (
    spark.read.format("parquet")
    .schema(os_schema)
    .load("dbfs:/mnt/lab/restricted/ELM-Project/stg/os-ngd-2022.parquet/layer=wtr_*")
    .filter('description != "Waterbody Catchment"')
    .select(
        "layer",
        "theme",
        "description",
        "watermark",
        "width",
        F.expr("ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry"),
    )
    .withColumn(
        "geometry",
        F.expr("CASE WHEN (width IS NOT NULL) THEN ST_Buffer(geometry, width) ELSE geometry END"),
    )
    .transform(to_gpq, "dbfs:/mnt/lab/restricted/ELM-Project/ods/os-water-2022.parquet")
)


sdf_os_wall = (
    spark.read.format("parquet")
    .schema(os_schema)
    .load("dbfs:/mnt/lab/restricted/ELM-Project/stg/os-ngd-2022.parquet")
    .filter('description REGEXP " Wall"')
    .select(
        "layer",
        "theme",
        "description",
        F.expr("ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry"),
    )
    .transform(to_gpq, "dbfs:/mnt/lab/restricted/ELM-Project/ods/os-wall-2022.parquet")
)

# COMMAND ----------

# gsjoin
drains = ["Ditch", "Named Ditch", "Moat"]
sdf_parcel = load_sdf("rpa-parcel-adas").select("id_parcel", "geometry")
sdf_hedge = load_sdf("rpa-hedge").select("geometry")
sdf_water = load_sdf("os-water-2022").filter(~F.col("description").isin(drains)).select("geometry")
sdf_ditch = load_sdf("os-water-2022").filter(F.col("description").isin(drains)).select("geometry")
sdf_wall = load_sdf("os-wall-2022").select("geometry")


for name, sdf_other in {
    "hedge": sdf_hedge,
    "water": sdf_water,
    "ditch": sdf_ditch,
    "wall": sdf_wall,
}.items():
    sdf = (
        sjoin(
            sdf_parcel,
            sdf_other.withColumn("geometry", F.expr("ST_SubDivideExplode(geometry, 256)")),
            rsuffix="",
            distance=12,
        )
        .transform(st_union, "id_parcel")
        .transform(
            to_gpq,
            f"dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-{name}-2024_01_08.parquet",
        )
    )

# COMMAND ----------

# metrics
f = "/dbfs/mnt/lab/restricted/ELM-Project/out/elmo-buffers-2024_01_08.feather"
sdf_parcel = load_sdf("rpa-parcel-adas").drop("sindex")
names = "hedge", "water"  # , 'ditch', 'wall'
bufs = 0, 4, 8, 12


null = 'ST_GeomFromText("Point EMPTY")'
sdf_geoms = sdf_parcel
for name in names:
    sdf_other = load_sdf(f"elmo_geo-{name}-2024_01_08").drop("sindex").withColumnRenamed("geometry", f"geometry_{name}")
    if name != "hedge":
        sdf_other = sdf_other.withColumn(f"geometry_{name}", F.expr(f"ST_Buffer(geometry_{name},  1)"))
    sdf_geoms = sdf_geoms.join(sdf_other, on="id_parcel", how="left").withColumn(f"geometry_{name}", F.expr(f"COALESCE(geometry_{name}, {null})"))

sdf = sdf_geoms.select(
    "id_parcel",
    *(F.expr(f"ST_Length(ST_MakeValid(ST_Intersection(geometry, geometry_{name}))) / 2 AS m_{name}") for name in names),
    *(
        F.expr(f"ST_Area(ST_MakeValid(ST_Intersection(geometry, ST_MakeValid(ST_Buffer(geometry_{name}, {buf}))))) / 10000 AS ha_{name}_buf{buf}m")
        for name in names
        for buf in bufs
    ),
)


df = sdf.toPandas()
df.to_feather(f)
df

# COMMAND ----------

# summary
{
    k: v
    for col in ("hedge", "water", "ditch", "wall")
    for k, v in {
        "parcels": f"{df['id_parcel'].nunique():,}",
        f"parcels with {col}": f"{(0 < df[f'm_{col}']).mean():.1%}",
        f"total m_{col}": f"{df[f'm_{col}'].sum() / 1e6:,.1f} Mm",
        f"total ha_{col}_buf0m": f"{df[f'ha_{col}_buf0m'].sum() / 1e6:,.1f} ha",
        f"total ha_{col}_buf4m": f"{df[f'ha_{col}_buf4m'].sum() / 1e6:,.1f} ha",
        f"total ha_{col}_buf8m": f"{df[f'ha_{col}_buf8m'].sum() / 1e6:,.1f} ha",
        f"total ha_{col}_buf12m": f"{df[f'ha_{col}_buf12m'].sum() / 1e6:,.1f} ha",
    }.items()
}

# COMMAND ----------

# Download

# Metrics
f_in = "/dbfs/mnt/lab/restricted/ELM-Project/out/elmo-buffers-2024_01_08.feather"
f_out = "/tmp/elmo-buffers-2024_01_08.csv"
pd.read_feather(f_in).to_csv(f_out)
download_link(f_out)

# Geometries
for name in "hedge", "water":
    f_in = f"/dbfs/mnt/lab/restricted/ELM-Project/ods/elmo_geo-{name}-2024_01_08.parquet"
    f_out = f"/tmp/elmo_geo-{name}-2024_01_08.gpkg"
    # gpd.read_parquet(f_in).set_crs(epsg=27700).drop(columns=['sindex']).to_file(f_out)
    download_link(f_out)
