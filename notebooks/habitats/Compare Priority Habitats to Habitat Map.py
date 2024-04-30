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

import numpy as np
import pandas as pd
from dataclasses import dataclass
import matplotlib.pyplot as plt
import matplotlib.ticker as tick
import matplotlib.gridspec as gridspec
import seaborn as sns

import elmo_geo
from elmo_geo.datasets.datasets import datasets
from elmo_geo.st import sjoin
from elmo_geo.st.geometry import load_geometry, load_missing

elmo_geo.register()
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
    'Purple moor grass and rush pastures':'Acid, Calcareous, Neutral Grassland',
    'Coastal vegetated shingle':'Bare Sand',
    'Lowland dry acid grassland':'Acid, Calcareous, Neutral Grassland',
    'Lowland meadows':'Acid, Calcareous, Neutral Grassland',
    'Lowland fens':'Fen, Marsh and Swamp',
    'Coastal saltmarsh':'Coastal Saltmarsh',
    'Grass moorland':'Acid, Calcareous, Neutral Grassland',
    'Limestone pavement':'Bare Ground',
    'Lowland calcareous grassland':'Acid, Calcareous, Neutral Grassland',
    'Mountain heaths and willow scrub':'Scrub',
    'Good quality semi-improved grassland':'Improved Grassland',
    'Lowland raised bog':'Bog',
    'Upland heathland':'Dwarf Shrub Heath',
    'Reedbeds':'Fen, Marsh and Swamp',
    'Lowland heathland':'Dwarf Shrub Heath',
    'Upland calcareous grassland':'Acid, Calcareous, Neutral Grassland',
    'Calaminarian grassland':'Acid, Calcareous, Neutral Grassland',
    'Blanket bog':'Bog',
    'Traditional orchard':'Arable and Horticultural',
    'Upland hay meadow':'Acid, Calcareous, Neutral Grassland',
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
df_comp["A_pred_from_Main_Habit"] = df_comp["Main_Habit"].map(habitat_type_lookup.get)
df_comp["Main_Habit_from_A_pred"] = df_comp["A_pred"].map({v:k for k,v in habitat_type_lookup.items()}.get) # this mappping systematically excludes some Main Habit classes but that doesn't affect the analysis

# COMMAND ----------

df_ph.drop_duplicates(subset="id_parcel").shape, df_ph.shape

# COMMAND ----------

df_hm.drop_duplicates(subset="id_parcel").shape, df_hm.shape

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

# for each dataset separaetely rank the frequency of each habitat type
ph_frequency = (df_ph
                .groupby("Main_Habit")
                .aggregate(
                    parcel_count = pd.NamedAgg("id_parcel", lambda s: s.nunique()),
                    proportion_main_habit_sum = pd.NamedAgg("proportion_main_habit", lambda s: s.sum()),
                )
                .reset_index()
                .sort_values(by="parcel_count")
                )

hm_frequency = (df_hm
                .groupby("A_pred")
                .aggregate(
                    parcel_count = pd.NamedAgg("id_parcel", lambda s: s.nunique()),
                    proportion_a_pred_sum = pd.NamedAgg("proportion_a_pred", lambda s: s.sum()),
                )
                .reset_index()
                .sort_values(by="parcel_count")
                )

# COMMAND ----------

ph_main_habits_not_mapped = df_comp.loc[ (df_comp["A_pred_from_Main_Habit"].isnull() & df_comp["Main_Habit"].notna())].groupby("Main_Habit")["id_parcel"].apply(lambda s: s.nunique()).sort_values().to_dict()

hm_a_pred_not_mapped = df_comp.loc[ (df_comp["Main_Habit_from_A_pred"].isnull() & df_comp["A_pred"].notna())].groupby("A_pred")["id_parcel"].apply(lambda s: s.nunique()).sort_values().to_dict()

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

frequency_comp = pd.merge(ph_frequency, hm_frequency, left_on = "A_pred_from_Main_Habit", right_on = "A_pred", how = "outer", suffixes = ("_ph", "_hm")).sort_values("parcel_count_hm", na_position='first')
frequency_comp["A_pred_plus_missing_Main_Habit"] = np.where(frequency_comp["A_pred"].isna(), frequency_comp["Main_Habit"], frequency_comp["A_pred"])
frequency_comp = frequency_comp.reset_index(drop=True)

# count number of time datasets agree on habitat types, including only parcels that overlap with both datasets
df_comp["habitats_match"] = df_comp["A_pred"] == df_comp["A_pred_from_Main_Habit"]

count_match = (df_comp
                .dropna(subset = ["Main_Habit", "A_pred"]) # only parcels with a habitat from both datasets
                .groupby(["id_parcel", "A_pred"])["habitats_match"].any() # count number of parcels that have a habitat match for each A_pre category
                .reset_index().groupby("A_pred")["habitats_match"].value_counts() # count total number of matches across haabitats
                .unstack() # pivot
)
count_match["true_pct"] = count_match[True]/count_match.sum(axis=1) * 100
count_match["false_pct"] = count_match[False]/count_match.sum(axis=1) * 100
count_match = count_match.sort_values(by="true_pct")
count_match

# COMMAND ----------

# create plots
fig = plt.figure(tight_layout=False, figsize = (30,30))
gs = gridspec.GridSpec(3, 2, height_ratios = [1,4,4])

defra_green = "#00A33B"
defra_green_dark = "#006123"
grey = "#8a836e"
dary_grey = "#454137"

title_size = 30
title_y = 1.03

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
ax0.xaxis.set_major_formatter(tick.FuncFormatter("{:.0f}%".format))
ax0.set_title("1. Parcels overlapping a habitat geometry", y=title_y)

# add data labels
for i, v1 in enumerate(b1.datavalues):
    s = f"{v1:.0f}%"
    ax0.text(v1-(2*len(s)), i-0.1, s, c = dary_grey, transform=ax0.transData)

# COMMAND ----------


# plots showing which habitats most commonly overlap with parcels acording to each dataset
sns.set(
    context="talk",
    style="white",
    palette="husl",
    font_scale=1.2,
)

ax1 = fig.add_subplot(gs[1, 0])
ax1.barh(ph_frequency["Main_Habit"], ph_frequency["parcel_count"], color = defra_green)
ax1.xaxis.set_major_formatter(tick.FuncFormatter(lambda x, y: f"{x/1_000:.0f}k"))
ax1.set_title("2. Priority Habitats parcel count", fontsize = title_size, y = title_y)

ax2 = fig.add_subplot(gs[1, 1])
ax2.barh(hm_frequency["A_pred"], hm_frequency["parcel_count"], color = defra_green)
ax2.xaxis.set_major_formatter(tick.FuncFormatter(lambda x, y: f"{x/1e6:.1f}m"))
ax2.set_title("3. Habitat Map parcel count", fontsize = title_size, y = title_y)


# COMMAND ----------

# plots comparing habitat classifications between datasets
sns.set(
    context="talk",
    style="white",
    palette="husl",
    font_scale=1.2,
)

# spine chart comparing number of parcels matching certain classifications under each dataset
ax3 = fig.add_subplot(gs[2, 0])
b2 = ax3.barh(frequency_comp["A_pred_plus_missing_Main_Habit"], frequency_comp["parcel_count_hm"].fillna(0) * -1, color = defra_green)
b1 = ax3.barh(frequency_comp["A_pred_plus_missing_Main_Habit"], frequency_comp["parcel_count_ph"].fillna(0), color = defra_green_dark)
ax3.legend([b1, b2], ["Priority Habitats", "Habitat Map"], loc="lower left")
ax3.xaxis.set_major_formatter(tick.FuncFormatter(lambda x, y: f"{x/1e6:.1f}m".replace("-","")))
ax3.set_title("4. Comparison parcel count", fontsize = title_size, y = title_y)

# addtext indicating where there is no matching habitat
y_labels = ax3.get_yticklabels()
for ix, r in frequency_comp.iterrows():
    name = r["A_pred_plus_missing_Main_Habit"]
    label = [i for i in y_labels if i.get_text() == name][0]
    if np.isnan(r["parcel_count_hm"]):
        ax3.text(0-50_000, label.get_position()[1]-0.2, "NA", ha = "right", transform=ax3.transData, fontsize = 20)
    elif np.isnan(r["parcel_count_ph"]):
        ax3.text(0+50_000, label.get_position()[1]-0.2, "NA", ha = "left", transform=ax3.transData, fontsize = 20)


# percentage of matching habitat classification
count_match = count_match.dropna()
ax4 = fig.add_subplot(gs[2, 1])
b1 = ax4.barh(count_match.index, count_match["true_pct"], color = defra_green)
b2 = ax4.barh(count_match.index, count_match["false_pct"], left=count_match["true_pct"], color=grey)
ax4.legend([b1, b2], ["Matching", "Not matching"], loc="lower right")
ax4.xaxis.set_major_formatter(tick.FuncFormatter("{:.0f}%".format))
ax4.set_title("5. Comparison habitat classification", fontsize = title_size, y = title_y)

# add data labels
for i, v1 in enumerate(b1.datavalues):
    s = f"{v1:.0f}%"
    ax4.text(v1+1, i-0.1, s, c = dary_grey, transform=ax4.transData)

for ax in [ax1, ax2, ax3]:
    ax.grid( which = "major", axis="x")

# COMMAND ----------

fig.text(0, -0.52, 
"""
1. Parcels overlapping a habitat geometry

The Living England Habitat Map is exhaustive and mutually exclusive; it covers the whole country with non-overlapping polygons with habitat classifications. Therefore, this dataset overlaps with 
100% of RPA parcel geometries. Conversely, the Priority Habitats dataset is non-exhaustive. It does not attempt to map every habitat and covers just over a third of parcels.

2. Priority Habitats parcel count

The Priority Habitats dataset has a larger number of more specific habitat types. Due to its non-exhaustive nature. All habitat types apart from deciduous woodland are found in less than 100,000 
parcels.

3. Habitat Map parcel count

Living England's Habitat Map uses a smaller number of boarder categories. The number of parcels in each category is high, with many parcels overlapping multiple habitat types (for reference there 
are 2.6m parcels).

4. Comparison parcel count

By assigning one of the broader Habitat Map classifications to each of the Priority Habitat classifications we can compare the number of parcels linked to these habitats according to each dataset. This 
chart shows that the Living England Habitat Map contains a different distribution of habitats, as well as greater overall coverage. For example, 'Broadleaved, Mixed and Yew Woodland' is the most frequent 
category according to the Priority Habitats data but this is third most frequent according to the Habitat Map.

NA annotations indicate habitat categories which do not have an equivalent in the comparison dataset. The following Priority Habitat types have no Habitat Map equivalent:
 - No main habitat but additional habitats present (94,138 parcels)
 - Maritime cliffs and slopes (5,087 parcels)

The following Habitat Map types have no equivalent in Priority Habitats:
- Built-up Araes and Gardens (303,195 parcels)
- Coniferous Woodland (119,523 parcels)
- Bracken (56,524 parcels)
- Unclassified (23,307 parcels) 

5. Percentage matching habitat type

This chart compares the classifications themselves rather than the volume of classifications. Only parcels with both a Priority Habitat and Habitat Map classification. Furthermore for 
each category the number of matching classifications is defined as the number of parcels where at least one of its habitats match between the datasets. This is a more permissive definition that 
aims to control for the Priority Habitat data potentially having overlapping classifications.

The results show good agreement in the classification of woodland and saltmarsh but generally poor agreement otherwise.

The level of disagreement is sensitive to the mapping between the two sets of habitat classes. Broader groupings (e.g. into grassland, woodland, scrub, bog, other) might be more suitable.

Sources: Defra Priority Habitats, Living England Habitat Map, RPA Parcels November 2021.
"""
,
fontsize = 20,
color = dary_grey)

fig.suptitle("Comparing Defra Priority Habitat data to the Living England Habitat Map", fontsize = 60, y = 1.035)

# COMMAND ----------

fig
