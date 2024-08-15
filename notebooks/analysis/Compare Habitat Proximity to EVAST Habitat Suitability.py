# Databricks notebook source
# MAGIC %md
# MAGIC # Compare Habitat Proximity to EVAST Habitat Suitability
# MAGIC
# MAGIC Author: Obi Thompson Sargoni
# MAGIC
# MAGIC Date: 15/08/2024
# MAGIC
# MAGIC This notebook is a brief comparison of two methods for classifying which parcels are suitable for different types of habitat creation.
# MAGIC
# MAGIC I compare two parcel levels datasets:
# MAGIC     - BTO habitats: this is used by the EVAST team to assign a habitat creation type to parcels.
# MAGIC       These in turn inform stocking densities on those parcels in the habitat creation scenario.
# MAGIC     - Grassland/heathland proximity: this is used by the FCP team to determine parcel eligibility
# MAGIC       for habitat creation/restoration actions.
# MAGIC
# MAGIC
# MAGIC The BTO dataset is based on spatially coarse SoilScapes soil classifications while the habitat proximity datasets are based on
# MAGIC distance to priority habitats.

# COMMAND ----------

import pandas as pd

from elmo_geo import register

# from elmo_geo.datasets import defra_heathland_proximity_parcels, defra_grassland_proximity_parcels

register()

# COMMAND ----------

f_grassland_proximity = "/dbfs/mnt/lab/unrestricted/ELM-Project/silver/defra/defra_grassland_proximity_parcels-2024_08_05-bf002535.parquet"
f_heathland_proximity = "/dbfs/mnt/lab/unrestricted/ELM-Project/silver/defra/defra_heathland_proximity_parcels-2024_08_05-bf002535.parquet"

f_bto_habitats = "/Workspace/Users/obi.thompsonsargoni@defra.gov.uk/EVAST/bto_soilscapes_out.csv"

# COMMAND ----------

df_g = pd.read_parquet(f_grassland_proximity)
df_g.head()

# COMMAND ----------

df_bto = pd.read_csv(f_bto_habitats)
df_bto.head()

# COMMAND ----------

df_h = pd.read_parquet(f_heathland_proximity)
df_h.head()

# COMMAND ----------

# Compare grassland first
print(df_bto["int_srg"].value_counts())
print(df_g["Main_Habit"].value_counts())

# COMMAND ----------

print(df_bto["int_heathland"].value_counts())
print(df_h["Main_Habit"].value_counts())

# COMMAND ----------

grassland_map = {
    "lowland_meadow": "Lowland meadows",
    "lowland_calc_gr": "Lowland calcareous grassland",
    "lowland_acid_gr": "Lowland dry acid grassland",
    "lowland_dry_acid_gr": "Lowland dry acid grassland",
    "upland_calc_gr": "Upland calcareous grassland",
}

heathland_map = {
    "lowland": "Lowland heathland",
    "upland": "Upland heathland",
}

# missing from bto: upland_meadow
# unused from phi: Grass moorland, Purple moor grass and rush pastures

# COMMAND ----------

print(df_bto["RLR_RW_R_5"].duplicated().value_counts())
print(df_g["id_parcel"].duplicated().value_counts())

# COMMAND ----------

df_g = df_g.groupby(["id_parcel", "distance"])["Main_Habit"].apply(lambda s: "-".join(s)).reset_index()
df_h = df_h.groupby(["id_parcel", "distance"])["Main_Habit"].apply(lambda s: "-".join(s)).reset_index()
print(df_g["id_parcel"].duplicated().value_counts())
print(df_h["id_parcel"].duplicated().value_counts())

# COMMAND ----------


def produce_summary(df_comp, grassland_map, bto_col):
    prop_bto_parcels_unmatched = df_comp["RLR_RW_R_5"].isnull().sum() / df_bto.shape[0]
    prop_phi_parcels_unmatched = df_comp["id_parcel"].isnull().sum() / df_g.shape[0]

    print(f"Percentage of parcels in BTO unmatched: {prop_bto_parcels_unmatched:.1%}")
    print(f"Percentage of parcels in PHI unmatched: {prop_phi_parcels_unmatched:.1%}")

    df_comp_matched = df_comp.loc[(~df_comp["id_parcel"].isnull()) & (~df_comp["RLR_RW_R_5"].isnull())]
    df_comp_matched[f"{bto_col}_phi"] = df_comp_matched[bto_col].replace(grassland_map)

    df_comp_matched_comparable = df_comp_matched.loc[df_comp_matched[bto_col].isin(list(grassland_map.keys()))]
    # df_comp_matched_incomparable = df_comp_matched.loc[ ~df_comp_matched[bto_col].isin(list(grassland_map.keys()))]

    df_comp_matched_comparable["match"] = df_comp_matched_comparable[f"{bto_col}_phi"] == df_comp_matched_comparable["Main_Habit"]
    df_comp_matched_comparable["match_part"] = df_comp_matched_comparable.apply(lambda row: row[f"{bto_col}_phi"] in row["Main_Habit"], axis=1)

    df_res = df_comp_matched_comparable.groupby(bto_col)["match"].apply(lambda s: s.value_counts()).unstack()
    df_res.loc["total"] = df_res.sum()
    df_res["pcnt_match"] = df_res[True] / df_res.sum(axis=1)

    return df_res


# COMMAND ----------

df_comp = pd.merge(df_g, df_bto, left_on=["id_parcel"], right_on=["RLR_RW_R_5"], how="outer")
df_res = produce_summary(df_comp, grassland_map)


# COMMAND ----------


df_res

# COMMAND ----------

df_comp_h = pd.merge(df_h, df_bto, left_on=["id_parcel"], right_on=["RLR_RW_R_5"], how="outer")
df_res_h = produce_summary(df_comp_h, heathland_map, bto_col="int_heathland")

# COMMAND ----------

df_res_h
