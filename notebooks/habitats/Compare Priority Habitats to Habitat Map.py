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

import elmo_geo
from elmo_geo.datasets.datasets import datasets
from elmo_geo.st import sjoin
from elmo_geo.st.geometry import load_geometry, load_missing

elmo_geo.register()
from pyspark.sql import functions as F

# COMMAND ----------

import sedona
sedona.version

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

# get priority habitat classes
ph_habitats = sdf_ph.select("Main_Habit").dropDuplicates().toPandas()
ph_habitats

# COMMAND ----------

ph_habitats['Main_Habit'].to_list()

# COMMAND ----------

# habitat map categories
@dataclass
class Habitat():
    name: str
    name_id: int
    exp_risk: int
    colour: tuple[int, int, int]

    def rgb_to_hex(self, r, g, b):
        return '#{:02x}{:02x}{:02x}'.format(r, g, b)
    
    @property
    def colour_hex(self):
        return self.rgb_to_hex(*self.colour)

habitats = [
    Habitat(name = 'Broadleaved, Mixed and Yew Woodland',
            name_id = 0, 
            exp_risk = 2, 
            colour = (51, 160, 44),
            ),
    Habitat(name = 'Coniferous Woodland',
            name_id = 1, 
            exp_risk = 4, 
            colour = (0, 80, 0),
            ),
    Habitat(name = 'Arable and Horticultural',
            name_id = 2, 
            exp_risk = 3, 
            colour = (240, 228, 66),
            ),
    Habitat(name = 'Improved Grassland',
            name_id = 3, 
            exp_risk = 3, 
            colour = (1, 255, 124),
            ),
    Habitat(name =  'Acid, Calcareous, Neutral Grassland',
            name_id = 4, 
            exp_risk = 3, 
            colour = (220, 153, 9),
            ),
    Habitat(name =  'Fen, Marsh and Swamp',
            name_id = 5, 
            exp_risk = 0, 
            colour = (253, 123, 238),
            ),
    Habitat(name =  'Dwarf Shrub Heath',
            name_id = 6, 
            exp_risk = 5, 
            colour = (128, 26, 128),
            ),
    Habitat(name =  'Scrub',
            name_id = 7, 
            exp_risk = 5, 
            colour = (230, 140, 166),
            ),
    Habitat(name = 'Bracken',
            name_id = 8, 
            exp_risk = 5, 
            colour =  (255, 192, 55),
            ),
    Habitat(name = 'Bog',
            name_id = 9, 
            exp_risk = 1, 
            colour =  (205, 59, 181),
            ),
    Habitat(name = 'Bare Ground',
            name_id = 10, 
            exp_risk = 2, 
            colour = (210, 210, 255),
            ),
    Habitat(name = 'Water',
            name_id = 11, 
            exp_risk = 1, 
            colour = (0, 0, 255),
            ),
    Habitat(name = 'Coastal Sand Dunes',
            name_id = 12, 
            exp_risk = 1, 
            colour = (204, 179, 0),
            ),
    Habitat(name = 'Coastal Saltmarsh',
            name_id = 13, 
            exp_risk = 1, 
            colour =  (0, 0, 92),
            ),
    Habitat(name = 'Bare Sand',
            name_id = 14, 
            exp_risk = 2, 
            colour = (255, 255, 128),
            ),
    Habitat(name = 'Built-up Areas and Gardens',
            name_id = 15, 
            exp_risk = 3, 
            colour = (0, 0, 0),
            ),
    Habitat(name = 'Unclassified',
            name_id = 16, 
            exp_risk = 2, 
            colour =  (128, 128, 12),
            ),
]

habitats

# COMMAND ----------

df = pd.DataFrame({"habitats":[h.name for h in habitats]})
df

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
    'No main habitat but additional habitats present':'',
    'Maritime cliff and slope':'',
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

sdf_comp = (sdf_parcels.select("id_parcel")
            .join(
                sdf_ph.select("id_parcel", "Main_Habit"),
                on = "id_parcel",
                how="outer",

            )
            .join(
                sdf_hm.select("id_parcel", "A_pred", "B_pred"), 
                on="id_parcel",
                how = "outer",
                )
            )
sdf_comp.display()

# COMMAND ----------

# compar covereage of each dataset by counting nulls
n_null_ph = sdf_comp.filter("Main_Habit is null").select("id_parcel").dropDuplicates().count()
n_null_hm = sdf_comp.filter("A_pred is null").select("id_parcel").dropDuplicates().count()
n_parcels = sdf_comp.select("id_parcel").dropDuplicates().count()
print(f"""
    Out of a total {n_parcels:,.0f}m parcels

    {n_null_ph:,.0f} ({n_null_ph/n_parcels:.0%}) do hot have a Priority Habitat habitat
    {n_null_hm:,.0f} ({n_null_hm/n_parcels:.0%}) do hot have a Habitat Map habitat
    """
)

# COMMAND ----------

# Compare counts of parcels and proportions by category
sdf_comp = (sdf_comp
            .withColumn("A_pred_from_Main_Habit"),
            sdf_comp.select("Main_Habit").replace(habitat_type_lookup)
            )
sdf_comp.display()

# COMMAND ----------


