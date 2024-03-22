# Databricks notebook source
# MAGIC %md
# MAGIC # Spatial Join lookup tables 

# COMMAND ----------

datasets

load_sdf



for dataset in datasets:
  sdf = (
    load_sdf(dataset.f_)
    .transform(sjoin, sdf_parcel, distance=12)
    .transform(to_gpq, dataset.f_lookup)
  )

# COMMAND ----------

# DASH
requirements = ['national_parks', 'ramsar', 'peaty_soils', 'national_character_areas', 'sssi', 'aonb', 'moorline', 'commons', 'flood_risk_areas']

dash_datalist = json.load('dash.json')
for data in dash_datalist:
    for req in requirements:
        if req in data["name"]:
            break
    requirements.remove(req)
    print(data)



