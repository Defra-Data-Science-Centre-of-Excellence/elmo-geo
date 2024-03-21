# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch from Data Sources

# COMMAND ----------

import json

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



