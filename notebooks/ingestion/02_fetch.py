# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch from Data Sources

# COMMAND ----------

import json
import os
import pandas as pd

def load_catalogue():
    catalogue


# COMMAND ----------

requirements = [
    'national_parks',
    'ramsar',
    'peaty_soils',
    'national_character_areas',
    'sssi', 
    'aonb',
    'moorline',
    'commons',
    'flood_risk_areas',
]

# COMMAND ----------

df = pd.concat([
    pd.read_json('dash.json'),
    pd.read_json('esri.json'),
])

df[['source', 'dataset', 'version']] = df['name'].str.split('-', n=2, expand=True)


display(df)

# COMMAND ----------

from elmo_geo.datasets.datasets import datasets
from dataclasses import dataclass, asdict

# {k: str(v) for k, v in asdict(datasets[0]).items()}
# # for dataset in datasets:
# #     dict

# datasets[0]




# COMMAND ----------

display(df[df['name'].str.contains('national_park')==True])

# COMMAND ----------

display(df[df['name'].str.contains('school')==True])

# COMMAND ----------

display(df[df['Error']==True][['service', 'dataset']])

# COMMAND ----------

import json

catalogue = json.load('catalogue.json')


data

catalogue.update(**dataset)


# COMMAND ----------

# DASH
requirements = [

dash_datalist = json.load('dash.json')
for data in dash_datalist:
    for req in requirements:
        if req in data["name"]:
            break
    requirements.remove(req)
    print(data)




# COMMAND ----------

# ESRI

# COMMAND ----------

# OS
os_key = os.environ['OS_KEY']

# COMMAND ----------

# OSM


# COMMAND ----------

# Manual
'''
rpa-parcel-adas
'''
