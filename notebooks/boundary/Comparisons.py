# Databricks notebook source
# MAGIC %md
# MAGIC # Comparisons
# MAGIC Comparing different Spatial Eligibilities against one another

# COMMAND ----------

import pandas as pd

"boundary,waterbody,drain,hedgerows,wall,urban,parcel_wfm"
"aonb,flood_risk_areas,lfa,met_office,moorland,national_character_areas,national_park,priority_habitats,ramsar,region,sssi,farm_type"

"ewco/woodland,peatland,wetland/habitat_suitability_map"
sources = "ewco/evast/habitat_suitability_map"


# COMMAND ----------

f = "/dbfs/mnt/lab/unrestricted/elm/uptake/elmo_features/2023_03_17.feather"
f = "/dbfs/mnt/lab/unrestricted/elm/uptake/elmo_features_long/2023-03-29.parquet"
df = pd.read_parquet(f)
df

# COMMAND ----------


f_farm = "/dbfs/mnt/lab/unrestricted/elm/wfm/2023_06_09/wfm_farms.feather"
f_parcel = "/dbfs/mnt/lab/unrestricted/elm/wfm/2023_06_09/wfm_parcels.feather"
f_hedge = "/dbfs/mnt/lab/unrestricted/elm/buffer_strips/hedgerows.parquet"
f_wall = "/dbfs/mnt/lab/unrestricted/elm/buffer_strips/wall.feather"
f_water = "/dbfs/mnt/lab/unrestricted/elm/elm_se/waterbody.parquet"
f_drain = "/dbfs/mnt/lab/unrestricted/elm/elm_se/drain.parquet"
f_region = "/dbfs/mnt/lab/unrestricted/elm/elmo/region/region.feather"
f_priority_habitat = "/dbfs/mnt/lab/unrestricted/elm/elmo/priority_habitats/output.feather"

df_farm = pd.read_feather(f_farm)
df_parcel = pd.read_feather(f_parcel)
df_hedge = pd.read_parquet(f_hedge)
df_wall = pd.read_feather(f_wall)
df_water = pd.read_parquet(f_water)
df_drain = pd.read_parquet(f_drain)
df_region = pd.read_feather(f_region)
df_priority_habitat = pd.read_feather(f_priority_habitat)

parcel = df_parcel[["id_business", "id_parcel"]]  # Ensure all cats are this long

# COMMAND ----------

bufs = 4, 6, 8, 10, 12
pd.DataFrame(
    {
        "Buffer": bufs,
        "Waterbody": [df_water.query(f"ha_water_buf{buf} > 0").shape[0] for buf in bufs],
        "Drain": [df_drain.query(f"ha_water_buf{buf} > 0").shape[0] for buf in bufs],
    },
)

# COMMAND ----------

buf = 12

hedge = (
    parcel.merge(df_hedge, how="left")
    .assign(
        feature="hedge",
        count=lambda df: df[f"sqm_buf{buf}"] > 0,
        hectarage=lambda df: df[f"sqm_buf{buf}"].fillna(0) / 10_000,
        boundary=lambda df: df["m_adj"].fillna(0),
    )
    .melt(
        id_vars=["id_parcel", "feature"],
        value_vars=["count", "hectarage", "boundary"],
        var_name="metric",
    )
)

wall = (
    parcel.merge(df_wall, how="left")
    .assign(
        feature="wall",
        count=lambda df: df[f"sqm_buf{buf}"] > 0,
        hectarage=lambda df: df[f"sqm_buf{buf}"].fillna(0) / 10_000,
        boundary=lambda df: df["m_wall"].fillna(0),
    )
    .melt(
        id_vars=["id_parcel", "feature"],
        value_vars=["count", "hectarage", "boundary"],
        var_name="metric",
    )
)

water = (
    parcel.merge(df_water, how="left")
    .assign(
        feature="water",
        count=lambda df: df[f"ha_water_buf{buf}"] > 0,
        hectarage=lambda df: df[f"ha_water_buf{buf}"].fillna(0),
        length=lambda df: df[f"m_water_buf{buf}"].fillna(0),
        boundary=lambda df: df[f"m_boundary_buf{buf}"].fillna(0),
    )
    .melt(
        id_vars=["id_parcel", "feature"],
        value_vars=["count", "hectarage", "length", "boundary"],
        var_name="metric",
    )
)

drain = (
    parcel.merge(df_drain, how="left")
    .assign(
        feature="drain",
        count=lambda df: df[f"ha_water_buf{buf}"] > 0,
        hectarage=lambda df: df[f"ha_water_buf{buf}"].fillna(0),
        length=lambda df: df[f"m_water_buf{buf}"].fillna(0),
        boundary=lambda df: df[f"m_boundary_buf{buf}"].fillna(0),
    )
    .melt(
        id_vars=["id_parcel", "feature"],
        value_vars=["count", "hectarage", "length", "boundary"],
        var_name="metric",
    )
)


df_base = pd.concat(
    [
        hedge,
        wall,
        water,
        drain,
    ],
)

df_base

# COMMAND ----------

region = parcel.merge(df_region, how="left").assign(
    category_group="region",
    category=lambda df: df["region"],  # fillna failed
)[["id_parcel", "category_group", "category"]]

farm_type = parcel.merge(df_farm, how="left").assign(
    category_group="farm_type",
    category=lambda df: df["farm_type"]
    .map(
        {
            0: None,
            1: "Cereals",
            2: "General Cropping",
            6: "Dairy",
            7: "LFA Grazing",
            8: "Lowland Grazing",
            9: "Mixed",
        },
    )
    .fillna("None"),
)[["id_parcel", "category_group", "category"]]

priority_habitat = (
    parcel.merge(df_priority_habitat, how="left")
    .query('confidence != "Low"')
    .assign(
        category_group="priority_habitat",
        category=lambda df: df["Main_Habit"].fillna("None"),
    )[["id_parcel", "category_group", "category"]]
)

priority_habitat_low = parcel.merge(df_priority_habitat, how="left").assign(
    category_group="priority_habitat_with_low",
    category=lambda df: df["Main_Habit"].fillna("None"),
)[["id_parcel", "category_group", "category"]]


df_cats = pd.concat(
    [
        region,
        farm_type,
        priority_habitat,
        priority_habitat_low,
    ],
)

df_cats

# COMMAND ----------

df = df_cats.merge(df_base).groupby(["feature", "category_group", "category", "metric"])["value"].agg(["sum", "mean"]).reset_index()

df

# COMMAND ----------

pd.options.display.float_format = "{:.3f}".format
p = (
    df.pivot(
        index=["category_group", "category"],
        columns=["feature", "metric"],
        values=["mean", "sum"],
    )
    .reorder_levels(["category_group", "category"], axis=0)
    .reorder_levels(["feature", "metric", None], axis=1)
    .T.sort_index()
)
p

# COMMAND ----------

display(p.reset_index())
