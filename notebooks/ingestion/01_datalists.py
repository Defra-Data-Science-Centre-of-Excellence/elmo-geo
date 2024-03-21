# Databricks notebook source
# MAGIC %md
# MAGIC # Data Listed from Data Sources

# COMMAND ----------

from datetime import datetime
from glob import iglob
import json
import re
import requests


def snake_case(string: str) -> str:
    """Convert string to snake_case
    1, lowercase
    2, replace spaces with underscores
    3, remove special characters
    \w=words, \d=digits, \s=spaces, [^ ]=not
    """
    return re.sub("[^\w\d_]", "", re.sub("\s", "_", string.lower()))

def string_to_dict(string: str, pattern: str) -> dict:
    """Revert f-string
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



# COMMAND ----------

# DASH
def gen_dash_datalist() -> tuple:
    """Generate a list of datasets in DASH.
    """
    # base snapshot
    pattern = '/dbfs/mnt/base/unrestricted/source_{source}/dataset_{dataset}/format_{format}/SNAPSHOT_{version}'
    for uri in iglob('/dbfs/mnt/base/unrestricted/source_*/dataset_*/format_*/SNAPSHOT_*'):
        d = string_to_dict(filepath, pattern)
        source, dataset, version, format = d.values()
        version = version.replace(f"_{dataset}", "")
        format = format.replace(f"_{dataset}", "")
        yield {
            "name": f"{source}-{dataset}-{version}",
            "url": uri,
            "fn": "dash",
            "format": format,
        }
    
    # base latest
    pattern = '/dbfs/mnt/base/unrestricted/source_{source}/dataset_{dataset}/format_{format}/SNAPSHOT_{version}'
    for uri in iglob('/dbfs/mnt/base/unrestricted/source_*/dataset_*/format_*/LATEST_*'):
        d = string_to_dict(uri, pattern)
        source, dataset, version, format = d.values()
        version = f"latest_{datetime.today().strftime('%Y_%m_%d')}"
        format = format.replace(f"_{dataset}", "")
        yield {
            "name": f"{d['source']}-{d['dataset']}-{d['version'].replace(f"_{d['dataset']}", "")}",
            "url": uri,
            "fn": "dash",
            "format": d["format"].replace(f"_{d['dataset']}", ""),
        }
    
    # lab elm
    pattern = '*/{source}/{dataset}/{version}.parquet'
    for uri in iglob('/dbfs/mnt/lab/unrestricted/elm*/**/*[!.snappy].parquet', recursive=True):
        d = string_to_dict(filepath, pattern)
        yield {
            "name": f"{d['source']}-{d['dataset']}-{d['version']}",
            "url": uri,
            "fn": "dash",
        }

type(gen_dash_datalist())
# save_json(list(gen_dash_datalist()), 'dash.json')

# COMMAND ----------

# ESRI
def get_esri_date(meta):
    ts = meta['editingInfo']['lastEditDate']
    dt = datetime.fromtimestamp(ts/1000) if ts else datetime.today()
    return dt.strftime("%Y_%m_%d")

def gen_esri_datalist():
    """Generate a list of ESRI datasets from 2 sources.
    Defra + Natural England's GeoPortal
    ONS's GeoPortal
    """
    for source, service in {
        'ons': "https://services1.arcgis.com/ESMARspQHYMw9BZ9/ArcGIS/rest/services",
        'defra': "https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services",
    }.items():
        for dataset in requests.get(f"{service}?f=pjson").json()["services"]:
            for layer in requests.get(f"{dataset['url']}?f=pjson").json()["layers"]:
                url = f"{dataset['url']}/{layer['id']}"
                meta = requests.get(f"{url}?f=pjson").json()
                yield {
                    "name": f"{source}-{snake_case(meta['name'])}-{get_esri_date(meta)}",
                    "url": url,
                    "fn": "esri",
                    "description": meta["description"],
                    "attribution": meta["copyrightText"],
                    # "meta": meta,
                }


save_json(list(gen_esri_datalist()), 'esri.json')

# COMMAND ----------

# OS


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
