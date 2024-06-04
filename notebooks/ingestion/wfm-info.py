# Databricks notebook source
from pyspark.sql import functions as F

from elmo_geo import register
from elmo_geo.datasets.catalogue import find_datasets
from elmo_geo.utils.misc import load_sdf

register()

# COMMAND ----------

sdf_parcel = load_sdf(find_datasets("rpa-parcel")[-1]["silver"])
sdf_wfm_field = load_sdf(find_datasets("wfm-field")[-1]["bronze"])

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
    sdf_wfm_field.fillna(0)
    .join(
        sdf_parcel.select("id_parcel", "geometry"),
        on = "id_parcel",
    )
    .selectExpr(
        "id_business",
        "id_parcel",
        "+".join(arable) + " AS ha_arable",
        "+".join(grassland) + " AS ha_grassland",
        "ST_Buffer(geometry, 0.001) AS geometry",  # Buffer for merging
    )
)


sdf.display()

# COMMAND ----------

sdf.selectExpr(
    "* EXCEPT(geometry)",
    "ST_AsText(ST_Centroid(geometry)) AS centroid",
).display()

# COMMAND ----------

sdf.groupby("id_business").agg(
    F.expr("SUM(ha_arable) AS ha_arable"),
    F.expr("SUM(ha_grassland) AS ha_grassland"),
    F.expr("ST_Union_Aggr(geometry) AS geometry"),
).selectExpr(
    "* EXCEPT(geometry)",
    "ST_X(ST_Centroid(geometry)) AS x",
    "ST_Y(ST_Centroid(geometry)) AS y",
).display()
