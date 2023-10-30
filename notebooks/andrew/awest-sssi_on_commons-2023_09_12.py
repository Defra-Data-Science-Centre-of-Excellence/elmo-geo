# Databricks notebook source
# MAGIC %md
# MAGIC # SSSI overlap Commons

# COMMAND ----------

# MAGIC %sh
# MAGIC esri2geojson $url $file_geojson
# MAGIC ogr2ogr $file_parquet $file_geojson -t_srs EPSG:27700 -f Parquet

# COMMAND ----------

import geopandas as gpd
import pandas as pd

f_sssi = "/dbfs/mnt/lab/restricted/ELM-Project/data/ne-sssi_units-2023_06_14.geojson"
f_commons = "/dbfs/mnt/lab/restricted/ELM-Project/data/ne-commons-2020_12_21.geojson"


# COMMAND ----------

gdf_sssi = (
    gpd.read_file(f_sssi)
    .to_crs(27700)[["OBJECTID", "SSSI_NAME", "CONDITION", "geometry"]]
    .rename(columns={"OBJECTID": "OBJECTID_sssi"})
)
gdf_commons = (
    gpd.read_file(f_commons)
    .to_crs(27700)[["OBJECTID", "geometry"]]
    .rename(columns={"OBJECTID": "OBJECTID_commons"})
)

# COMMAND ----------

# SSSI calculate proportion of SSSI Names in Unit
gdf_sssi_named = (
    gdf_sssi.assign(
        area=lambda df: df.area,
    )
    .pipe(
        lambda df: df.merge(
            df.groupby("SSSI_NAME")[["area"]].sum().rename(columns={"area": "area_name"}),
            on="SSSI_NAME",
        )
    )
    .assign(
        proportion_sssi_name=lambda df: df["area"] / df["area_name"],
    )
    .drop(columns=["area", "area_name", "SSSI_NAME"])
)

gdf_sssi_named

# COMMAND ----------

id_sssi_unit, id_common, condition, geometry_sssi, geomtry_common

# COMMAND ----------

# Long Table of SSSI on Common
df = (
    # Spatial Join
    gpd.GeoDataFrame.sjoin(
        gdf_sssi_named[["OBJECTID_sssi", "geometry"]],
        gdf_commons,
    )
    .drop(columns=["index_right", "geometry"])
    # Outer Join all columns
    .merge(gdf_sssi_named, on="OBJECTID_sssi", how="outer")
    .merge(gdf_commons, on="OBJECTID_commons", how="left", suffixes=("_sssi", "_commons"))
    # Merge Commons
    .groupby("OBJECTID_sssi")
    .agg(
        {
            "OBJECTID_commons": "count",  # count_unqiue = lambda x: pd.DataFrame(x).nunique()[0]
            "proportion_sssi_name": "first",
            "CONDITION": "first",
            "geometry_sssi": "first",  # unary_union=first as grouping by id sssi
            "geometry_commons": lambda x: gpd.GeoSeries(x).make_valid().unary_union,  # geom_union
        }
    )
    .rename(columns={"OBJECTID_commons": "count_commons"})
    .reset_index()
    # Split Intersection
    .assign(
        ha_on=lambda df: gpd.GeoSeries.intersection(
            gpd.GeoSeries(df["geometry_sssi"]).fillna().make_valid(),
            gpd.GeoSeries(df["geometry_commons"]).fillna().make_valid(),
        )
        .make_valid()
        .area
        / 10_000,
        ha_off=lambda df: gpd.GeoSeries.difference(
            gpd.GeoSeries(df["geometry_sssi"]).fillna().make_valid(),
            gpd.GeoSeries(df["geometry_commons"]).fillna().make_valid(),
        )
        .make_valid()
        .area
        / 10_000,
        ha=lambda df: df["ha_off"] + df["ha_on"],
        condition=lambda df: df["CONDITION"].str.title(),
        condition_code=lambda df: pd.Categorical(
            df["condition"],
            categories=[
                "Favourable",  # 0
                "Unfavourable Recovering",  # 1
                "Unfavourable No Change",  # 2
                "Unfavourable Declining",  # 3
                "Part Destroyed",  # 4
                "Destroyed",  # 5
                # 'Not Assessed',           # -1
                # '',                       # -1
            ],
            ordered=True,
        ).codes,
    )
    .drop(columns=["geometry_sssi", "geometry_commons", "CONDITION"])
)


df.to_feather("/dbfs/tmp/awest-sssi_on_commons-2023_09_05.feather")
df

# COMMAND ----------

# Grouped by Condition
df = pd.read_feather("/dbfs/tmp/awest-sssi_on_commons-2023_09_05.feather")


df_grouped = (
    df.assign(
        count_sssi_unit=1,
        count_sssi_unit_on_commons=lambda df: 0 < df["ha_on"],
    )
    .groupby("condition_code")
    .agg(
        {
            "condition": "first",
            "count_sssi_unit": "sum",
            "count_sssi_unit_on_commons": "sum",
            "count_commons": "sum",
            "ha_on": "sum",
            "ha_off": "sum",
            "ha": "sum",
        }
    )
    .round(1)
)


df_grouped

# COMMAND ----------

import pandas as pd
from statsmodels.formula.api import ols

df = pd.read_feather("/dbfs/tmp/awest-sssi_on_commons-2023_09_05.feather")
df["with"] = 0 < df["ha_on"]
df["p_ha"] = df["ha_on"] / df["ha"]
df["half_with"] = 0.5 < (df["ha_on"] / df["ha"])
df = df.query("condition_code!=-1 & condition_code!=4 & condition_code!=5")
df["ha_total"] = df["ha"]

model = ols(
    # formula = 'condition_code ~ ha_on * ha',
    # formula = 'condition_code ~ ha_on + ha_total',
    # formula = 'condition_code ~ with',
    # formula = 'condition_code ~ p_ha',
    formula="condition_code ~ half_with",
    data=df,
).fit()


print(model.summary())

# COMMAND ----------

sjoin(sssi, common).groupby("id_sssi").agg(first, union_aggr("geometry_commons")).select()
