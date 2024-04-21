# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Comparing Priority Habitats vs Living England Habitat Map
# MAGIC **Author**: Obi Thompson Sargoni
# MAGIC
# MAGIC Defra's Priority Habitas data and Living England's Habitat Map provide similar information.
# MAGIC
# MAGIC Both these datasets have been linked to WFM parcels (ADAS, November 2021). This notebook performs joins and mapping to compare theee sources of information on habitats.

# COMMAND ----------

import pandas as pd
from dataclasses import dataclass
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns

import elmo_geo
from elmo_geo.datasets.datasets import datasets
from elmo_geo.st import sjoin
from elmo_geo.st.geometry import load_geometry, load_missing

#elmo_geo.register()
from pyspark.sql import functions as F

# COMMAND ----------

ph_name = "priority_habitat_inventory"
ph_version = "2021_03_26"
ph_dataset = next(d for d in datasets if d.name == ph_name)
sf_priority_habitat = ph_dataset.path_output.format(version=ph_version)

hm_name = "habitat_map"
hm_version = "2022_09_16"
hm_dataset = next(d for d in datasets if d.name == hm_name)
sf_habitat_map = hm_dataset.path_output.format(version=hm_version)

sf_parcels = "dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet"

sf_priority_habitat, sf_habitat_map

# COMMAND ----------

sdf_parcels = spark.read.format("parquet").load(sf_parcels)

# COMMAND ----------

sdf_hm = spark.read.format("parquet").load(sf_habitat_map)
sdf_hm.display()

# COMMAND ----------

sdf_ph = spark.read.format("parquet").load(sf_priority_habitat)
sdf_ph.display()

# COMMAND ----------

# maps from priority habitat types (Main_Habit) to habitat map types (A_pred)
habitat_type_lookup = {
    'Deciduous woodland':'Broadleaved, Mixed and Yew Woodland',
    'Fragmented heath':'Dwarf Shrub Heath',
    'Coastal sand dunes':'Coastal Sand Dunes',
    'Saline lagoons':'Water',
    'Purple moor grass and rush pastures':'Arable and Horticultural',
    'Coastal vegetated shingle':'Bare Sand',
    'Lowland dry acid grassland':'Acid, Calcareous, Neutral Grassland',
    'Lowland meadows':'Acid, Calcareous, Neutral Grassland',
    'Lowland fens':'Fen, Marsh and Swamp',
    'Coastal saltmarsh':'Coastal Saltmarsh',
    'Grass moorland':'Arable and Horticultural',
    'Limestone pavement':'Bare Ground',
    'Lowland calcareous grassland':'Acid, Calcareous, Neutral Grassland',
    'Mountain heaths and willow scrub':'Scrub',
    'Good quality semi-improved grassland':'Improved Grassland',
    'Lowland raised bog':'Bog',
    'Upland heathland':'Dwarf Shrub Heath',
    'Reedbeds':'Fen, Marsh and Swamp',
    #'No main habitat but additional habitats present':'',
    #'Maritime cliff and slope':'',
    'Lowland heathland':'Dwarf Shrub Heath',
    'Upland calcareous grassland':'Acid, Calcareous, Neutral Grassland',
    'Calaminarian grassland':'Acid, Calcareous, Neutral Grassland',
    'Blanket bog':'Bog',
    'Traditional orchard':'Arable and Horticultural',
    'Upland hay meadow':'Arable and Horticultural',
    'Mudflats':'Bare Ground',
    'Upland flushes, fens and swamps':'Fen, Marsh and Swamp',
    'Coastal and floodplain grazing marsh':'Fen, Marsh and Swamp',
}

# COMMAND ----------

df_ph = (sdf_ph
         .select("id_parcel", "Main_Habit", "proportion")
         .groupby("id_parcel", "Main_Habit")
         .agg(F.sum("proportion").alias("proportion_main_habit"))
         .toPandas()
         )

df_hm = (sdf_hm
         .select("id_parcel", "A_pred", "proportion")
         .groupby("id_parcel", "A_pred")
         .agg(F.sum("proportion").alias("proportion_a_pred"))
         .toPandas()
         )


df_comp = (sdf_parcels.select("id_parcel").dropDuplicates().toPandas()
           .merge(df_ph, on="id_parcel", how = "outer")
           .merge(df_hm, on = "id_parcel", how = "outer")
)

df_comp.head(10)

# COMMAND ----------

# map from one habitat type to another
df_comp["A_pred_from_Main_Habit"] = df_comp["Main_Habit"].map(lambda x: habitat_type_lookup.get(x))
df_comp["Main_Habit_from_A_pred"] = df_comp["A_pred"].map(lambda x: {v:k for k,v in habitat_type_lookup.items()}.get(x))

# COMMAND ----------

df_ph.drop_duplicates(subset="id_parcel").shape

# COMMAND ----------

df_comp.drop_duplicates(subset="id_parcel")["Main_Habit"].isna().value_counts()

# COMMAND ----------

n_null_ph = df_comp.drop_duplicates(subset="id_parcel")["Main_Habit"].isna().value_counts()
n_null_ph.name = "Priority Habitat"
n_null_hm = df_comp.drop_duplicates(subset="id_parcel")["A_pred"].isna().value_counts()
n_null_hm.name = "Habitat Map"

print(f"""
    Out of a total {df_comp.drop_duplicates(subset="id_parcel").shape[0]:,.0f}m parcels

    {n_null_ph[True]:,.0f} ({n_null_ph[True]/n_null_ph.sum():.0%}) do hot have a Priority Habitat habitat
    {n_null_hm[True]:,.0f} ({n_null_hm[True]/n_null_hm.sum():.0%}) do hot have a Habitat Map habitat
    """
)

# COMMAND ----------

df_ph.head()

# COMMAND ----------

# for each dataset separaetely rank the frequency of each habitat type
ph_frequency = (df_ph
                .groupby("Main_Habit")
                .aggregate(
                    parcel_count = pd.NamedAgg("id_parcel", lambda s: s.count()),
                    proportion_main_habit_sum = pd.NamedAgg("proportion_main_habit", lambda s: s.sum()),
                )
                .reset_index()
                .sort_values(by="parcel_count")
                )

hm_frequency = (df_hm
                .groupby("A_pred")
                .aggregate(
                    parcel_count = pd.NamedAgg("id_parcel", lambda s: s.count()),
                    proportion_a_pred_sum = pd.NamedAgg("proportion_a_pred", lambda s: s.sum()),
                )
                .reset_index()
                .sort_values(by="parcel_count")
                )

# COMMAND ----------

ph_main_habits_not_mapped = df_comp.loc[ (df_comp["A_pred_from_Main_Habit"].isnull() & df_comp["Main_Habit"].notna())].groupby("Main_Habit")["id_parcel"].apply(lambda s: s.drop_duplicates().count()).sort_values().to_dict()

hm_a_pred_not_mapped = df_comp.loc[ (df_comp["Main_Habit_from_A_pred"].isnull() & df_comp["A_pred"].notna())].groupby("A_pred")["id_parcel"].apply(lambda s: s.drop_duplicates().count()).sort_values().to_dict()

nl = "\n - "
print(f"""
      
Priority Habitat habitat types not matched to Habitat Map types:
\n - {nl.join("{} ({:,})".format(k,v) for k,v in ph_main_habits_not_mapped.items())}

Habitat Map habitat types not matched to Priority Habitat types:
\n - {nl.join("{} ({:,})".format(k,v) for k,v in hm_a_pred_not_mapped.items())}
""")

# COMMAND ----------

# comparing classifications, how often do they match up
ph_frequency["A_pred_from_Main_Habit"] = ph_frequency["Main_Habit"].map(lambda x: habitat_type_lookup.get(x))

frequency_comp = pd.merge(ph_frequency, hm_frequency, left_on = "A_pred_from_Main_Habit", right_on = "A_pred", how = "outer")

# count number of time datasets agree on habitat types
df_comp["habitats_match"] = df_comp["A_pred"] == df_comp["A_pred_from_Main_Habit"]

count_match = df_comp.groupby("A_pred")["habitats_match"].value_counts().unstack()
count_match["true_pct"] = count_match[True]/count_match.sum(axis=1) * 100
count_match["false_pct"] = count_match[False]/count_match.sum(axis=1) * 100
count_match = count_match.sort_values(by="true_pct")
count_match

# COMMAND ----------

# create plots
fig = plt.figure(tight_layout=True, figsize = (30,40))
gs = gridspec.GridSpec(3, 2)

defra_green = "#00A33B"
grey = "#8a836e"

# COMMAND ----------

sns.set(
    context="talk",
    style="white",
    palette="husl",
    font_scale=2,
)

# horizontal bar chart showing how many parcels have habitats
ax0 = fig.add_subplot(gs[0, :])

df = pd.DataFrame([n_null_hm, n_null_ph])

dataset = df.index
without_habitat = df[True]/df.sum(axis=1) * 100
with_habitat = df[False]/df.sum(axis=1) * 100

b1 = ax0.barh(dataset, with_habitat, color=defra_green)
b2 = ax0.barh(dataset, without_habitat, left=with_habitat, color=grey)

ax0.legend([b1, b2], ["Overlapping habitat", "No habitat"], loc="upper right")

ax0.set_title("Percentage of parcels overlapping a habitat geometry")

plt.show()

# COMMAND ----------


# plots showing which habitats most commonly overlap with parcels acording to each dataset
sns.set(
    context="talk",
    style="white",
    palette="husl",
    font_scale=1.1,
)

ax1 = fig.add_subplot(gs[1, 0])
ax1.barh(ph_frequency["Main_Habit"], ph_frequency["parcel_count"], color = defra_green)
ax1.set_title("Priority Habitats Parcel Count")

ax2 = fig.add_subplot(gs[1, 1])
ax2.barh(hm_frequency["A_pred"], hm_frequency["parcel_count"], color = defra_green)
ax2.set_title("Habitat Map Parcel Count")


# COMMAND ----------

# plots comparing habitat classifications between datasets
sns.set(
    context="talk",
    style="white",
    palette="husl",
    font_scale=1.1,
)

ax3 = fig.add_subplot(gs[2, 0])
b1 = ax3.barh(count_match.index, count_match["true_pct"], color = defra_green)
b2 = ax3.barh(count_match.index, count_match["false_pct"], left=count_match["true_pct"], color=grey)
ax3.legend([b1, b2], ["Matching", "Not matching"], loc="upper right")
ax3.set_title("Percetage of parcels with matching habitat type")

# ax2 = fig.add_subplot(gs[2, 1])
# ax2.barh(hm_frequency["A_pred"], hm_frequency["parcel_count"], color = defra_green)
# ax2.set_title("Habitat Map Parcel Count")

# COMMAND ----------

fig
