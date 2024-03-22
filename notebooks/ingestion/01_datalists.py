# Databricks notebook source
# MAGIC %md
# MAGIC # Data Listed from Data Sources

# COMMAND ----------

from dataclasses import asdict
from datetime import datetime
from glob import iglob
import json
import os
import re
import requests

from elmo_geo.datasets.datasets import datasets


def snake_case(string: str) -> str:
    """Convert string to snake_case
    1, lowercase
    2, replace spaces with underscores
    3, remove special characters
    \w=words, \d=digits, \s=spaces, [^ ]=not
    """
    return re.sub("[^\w\d_]", "", re.sub("\s", "_", string.lower()))

def string_to_dict(string: str, pattern: str) -> dict:
    """Reverse f-string
    https://stackoverflow.com/a/36838374/10450752
    """
    regex = re.sub(r'{(.+?)}', r'(?P<_\1>.+)', pattern)
    return dict(zip(
        re.findall(r'{(.+?)}', pattern),
        list(re.search(regex, string).groups()),
    ))

def save_json(data: object, filepath: str) -> object:
    """Save a json file
    """
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    return data


os.chdir(os.getcwd().replace('/notebooks/ingestion', ''))
os.getcwd()

# COMMAND ----------

# Datasets
save_json(
    [asdict(dataset) for dataset in datasets],
    'data/catalogue.json',
)

# COMMAND ----------

import pandas as pd

df = pd.read_json('data/catalogue.json')

df

# COMMAND ----------

# DASH
def gen_dash_datalist() -> object:
    """Generate a list of datasets in DASH.
    """
    if True:  # base snapshot
        pattern = '/dbfs/mnt/base/{license}/source_{source}/dataset_{dataset}/format_{format}/SNAPSHOT_{version}'
        for uri in iglob('/dbfs/mnt/base/*/source_*/dataset_*/format_*/SNAPSHOT_*'):
            _, source, dataset, format, version = string_to_dict(uri, pattern).values()
            version = version.replace(f"_{dataset}", "")
            format = format.replace(f"_{dataset}", "")
            yield {
                "name": f"{source}-{dataset}-{version}",
                "url": uri,
                "fn": "dash",
                "format": format,
            }
    if True:  # base latest
        pattern = '/dbfs/mnt/base/{license}/source_{source}/dataset_{dataset}/format_{format}/LATEST_{version}'
        for uri in iglob('/dbfs/mnt/base/*/source_*/dataset_*/format_*/LATEST_*'):
            _, source, dataset, format, version = string_to_dict(uri, pattern).values()
            version = f"latest_{datetime.today().strftime('%Y_%m_%d')}"
            format = format.replace(f"_{dataset}", "")
            yield {
                "name": f"{source}-{dataset}-{version}",
                "url": uri,
                "fn": "dash",
                "format": format,
            }
    if False:  # lab elm
        pattern = 'elm*/{path}.parquet'
        for uri in iglob('/dbfs/mnt/lab/unrestricted/elm*/**/*[!.snappy].parquet', recursive=True):
            path, = string_to_dict(uri, pattern).values()
            yield {
                "name": path.replace('/', '-'),
                "url": uri,
                "fn": "dash",
            }


save_json(list(gen_dash_datalist()), 'data/dash.json')

# COMMAND ----------

# ESRI
def gen_esri_datalist() -> object:
    """Generate a list of avaiable datasets from 2 ESRI services.
    Defra + Natural England's GeoPortal
    ONS's GeoPortal
    """
    def get_esri_date(ts: int) -> str:
        return (datetime.fromtimestamp(ts/1000) if ts else datetime.today()).strftime("%Y_%m_%d")

    for source, service in {
        'ons': "https://services1.arcgis.com/ESMARspQHYMw9BZ9/ArcGIS/rest/services",
        'defra': "https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services",
    }.items():
        try:
            for dataset in requests.get(f"{service}?f=pjson").json()["services"]:
                try:
                    for layer in requests.get(f"{dataset['url']}?f=pjson").json()["layers"]:
                        try:
                            url = f"{dataset['url']}/{layer['id']}"
                            meta = requests.get(f"{url}?f=pjson").json()
                            yield {
                                "name": f"{source}-{snake_case(meta['name'])}-{get_esri_date(meta['editingInfo']['lastEditDate'])}",
                                "url": url,
                                "fn": "esri",
                                "description": meta["description"],
                                "attribution": meta["copyrightText"],
                                # "meta": meta,
                            }
                        except Exception:
                            yield {'Error':True, 'service':service, 'dataset':dataset, 'layer':layer}
                except Exception:
                    yield {'Error':True, 'service':service, 'dataset':dataset}
        except Exception:
            yield {'Error':True, 'service':service}


save_json(list(gen_esri_datalist()), 'data/esri.json')

# COMMAND ----------

# OS
from osdatahub import NGD




# COMMAND ----------

# OSM

# COMMAND ----------

# SharePoint


# COMMAND ----------

# Manual


"""
# Base
rpa-parcel
wfm-farm
wfm-field

# Features
alc
national_park
ramsar
peaty_soils
national_character_areas
sssi
aonb
lfa
region
commons
flood_risk_areas
ewco-red_squirrel
ewco-priority_habitat_network
nfc_social
ewco-water_quality
ewco-flood_risk_management
keeping_rivers_cool_riparian_buffers
nfc_ammonia_emmissions
tiles

# Boundary
rpa-hedge
os-ngd
osm-uk
he-shine

# Bare Soil

# Sylvan
"""
