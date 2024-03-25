# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch from Data Sources
# MAGIC Create a list of requirements, fetch and ingest the datasets.
# MAGIC
# MAGIC  with; name, method, source.
# MAGIC

# COMMAND ----------

from glob import glob
import json
import os
import osdatahub
import pandas as pd
# from elmo_geo.io import ingest_dash, ingest_esri, ingest_os, ingest_osm, ingest_spol
from elmo_geo.io.file import convert_file


def snake_case(string: str) -> str:
    """Convert string to snake_case
    1, lowercase
    2, replace spaces with underscores
    3, remove special characters
    \w=words, \d=digits, \s=spaces, [^ ]=not
    """
    return re.sub("[^\w\d_]", "", re.sub("[\s-]", "_", string.lower()))

def string_to_dict(string: str, pattern: str) -> dict:
    """Reverse f-string
    https://stackoverflow.com/a/36838374/10450752
    """
    regex = re.sub(r'{(.+?)}', r'(?P<_\1>.+)', pattern)
    return dict(zip(
        re.findall(r'{(.+?)}', pattern),
        list(re.search(regex, string).groups()),
    ))


# ingest_methods = {
#     "dash": ingest_dash,
#     "esri": ingest_esri,
#     "os": ingest_os,
#     "osm": ingest_osm,
#     "sharepoint": ingest_spol,
# }


def search_available(df, string):
    display(df[df['name'].str.contains(string)==True])


os.environ["OS_KEY"] = "WxgUdETn6cy58WZkfwZ7wdMVLlt5eDsX"



os.chdir(os.getcwd().replace('/notebooks/ingestion', ''))

catalogue = json.loads(open('data/catalogue.json').read())


# for dataset in catalogue:
#     ingest = ingest_methods[dataset['method']]
#     ingest(dataset)


# COMMAND ----------

catalogue = json.loads(open('data/catalogue.json').read())
catalogue

# COMMAND ----------

# DASH


# COMMAND ----------

# ESRI

# COMMAND ----------

help(osdatahub.DataPackageDownload)

# COMMAND ----------

# OS
os_key = os.environ['OS_KEY']
product_id = 4143

package = osdatahub.DataPackageDownload(os_key, product_id)
version = package.versions[0]
package.download(version["id"], output_dir="/tmp")

for f_in in glob("/tmp/*.zip"):
    f_stg = "{dir}/os-ngd-{date}.parquet/layer={layer}".format(
        dir = FOLDER_STG,
        date = snake_case(version["createdOn"]),
        layer = string_to_dict(f_in, "/tmp/{dataset}.zip").values()[0],
    )
    convert_file(f_in, f_stg)

# COMMAND ----------

version = package.versions[0]
for version_tmp in package.versions[1:]:
    if version_tmp["supplyType"] == "FULL":
        if version_tmp["createdOn"] > version["createdOn"]:
            version = version_tmp
version

# COMMAND ----------

# OSM

# COMMAND ----------

# SPOL
'''
rpa-parcel-adas
'''
