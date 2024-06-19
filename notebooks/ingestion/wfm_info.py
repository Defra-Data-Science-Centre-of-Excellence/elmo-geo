# Databricks notebook source
# MAGIC %md
# MAGIC # WFM Info
# MAGIC Calculate regularly used features from WFM, at both parcel and business level.
# MAGIC - ha_arable: the area used for arable farming
# MAGIC - ha_grassland: the area used for grazing
# MAGIC - x,y: centroid values for the parcel or business

# COMMAND ----------

from pyspark.sql import functions as F

from elmo_geo import register
from elmo_geo.datasets.catalogue import add_to_catalogue, find_datasets
from elmo_geo.utils.misc import load_sdf

register()

# COMMAND ----------

parcel = find_datasets("rpa-parcel")[-1]
wfm_farm = find_datasets("wfm-farm")[-1]
wfm_field = find_datasets("wfm-field")[-1]

version = wfm_field["name"].split("-")[2]
path = "/".join(wfm_field["bronze"].split("/")[:-2]) + "/silver"

sdf_parcel = load_sdf(parcel["silver"])
sdf_wfm = load_sdf(wfm_field["bronze"])

# COMMAND ----------

arable = [
    "ha_winter_wheat",
    "ha_spring_wheat",
    "ha_winter_barley",
    "ha_spring_barley",
    "ha_winter_oats",
    "ha_spring_oats",
    "ha_oilseed_rape",
    "ha_peas",
    "ha_field_beans",
    "ha_grain_maize",
    "ha_potatoes",
    "ha_sugar_beet",
    "ha_other_crop",
    "ha_fodder_maize",
    "ha_temporary_pasture",
]

grassland = [
    "ha_improved_grades_1_2",
    "ha_improved_grades_3_4_5",
    "ha_improved_disadvantaged",
    "ha_lowland_other",
    "ha_unimproved",
    "ha_unimproved_disadvantaged",
    "ha_disadvantaged",
    "ha_severely_disadvantaged",
    "ha_moorland",
    "ha_fenland",
]


sdf = (
    sdf_parcel
    .selectExpr(
        "id_parcel",
        "ST_CollectionExtract(geometry, 3) AS geometry",  # selecting polygons is better than buffering
    )
    .groupby("id_parcel")
    .agg(
        F.expr("ST_Union_Aggr(geometry) AS geometry"),
    )
    .join(
        sdf_wfm.fillna(0),
        on="id_parcel",
    )
    .selectExpr(
        "id_business",
        "id_parcel",
        "+".join(arable) + " AS ha_arable",
        "+".join(grassland) + " AS ha_grassland",
        "geometry",
    )
)


sdf.selectExpr("ST_GeometryType(geometry) AS gtype").groupby("gtype").count().display()
sdf.display()

# COMMAND ----------

name = f"wfm-field_info-{version}"
wfm_field["silver"] = f"{path}/{name}.parquet"


df = sdf.selectExpr(
    "* EXCEPT(geometry)",
    "ROUND(ST_X(ST_Centroid(geometry))) AS x",
    "ROUND(ST_Y(ST_Centroid(geometry))) AS y",
).toPandas()


assert df.shape[0] == df["id_parcel"].nunique()

df.to_parquet(wfm_field["silver"])
add_to_catalogue([wfm_field])

# COMMAND ----------

name = f"wfm-farm_info-{version}"
wfm_farm["silver"] = f"{path}/{name}.parquet"

df = (
    sdf.groupby("id_business")
    .agg(
        F.expr("SUM(ha_arable) AS ha_arable"),
        F.expr("SUM(ha_grassland) AS ha_grassland"),
        F.expr("ST_Collect(COLLECT_LIST(geometry)) AS geometry"),  # ST_Union_Aggr but keeps geometry separate
    )
    .selectExpr(
        "* EXCEPT(geometry)",
        "ROUND(ST_X(ST_Centroid(geometry))) AS x",
        "ROUND(ST_Y(ST_Centroid(geometry))) AS y",
    )
    .toPandas()
)


assert df.shape[0] == df["id_business"].nunique()

df.to_parquet(wfm_farm["silver"])
add_to_catalogue([wfm_farm])
