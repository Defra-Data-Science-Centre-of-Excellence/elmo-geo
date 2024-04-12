# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch
# MAGIC Complete the tasks for downloading and ingesting for catalogue datsets.  
# MAGIC
# MAGIC | Task | Last Completed | Notes |
# MAGIC |---|---|---|
# MAGIC | eods | | todo
# MAGIC | esri | 
# MAGIC | os |
# MAGIC | osm_pbf |
# MAGIC | spol | | todo
# MAGIC | ingest | | todo

# COMMAND ----------

from datetime import datetime
import esridump  # noqa
import fiona
from glob import iglob
import json
import mosaic as mos
import os
import osdatahub
from pyspark.sql import functions as F

from elmo_geo import register, LOG
from elmo_geo.utils.misc import dbfs, sh_run, snake_case


os.environ['OS_KEY'] =  'WxgUdETn6cy58WZkfwZ7wdMVLlt5eDsX'
DATE = datetime.today().strftime('%Y_%m_%d')
# RAW = '/Volumes/prd_dash_lab/elm_project_restricted/raw'
# BRONZE = '/Volumes/prd_dash_lab/elm_project_restricted/bronze'
# SILVER = '/Volumes/prd_dash_lab/elm_project_restricted/silver'
# GOLD = '/Volumes/prd_dash_lab/elm_project_restricted/gold'
RAW = '/tmp'
BRONZE = '/dbfs/mnt/lab/restricted/ELM-Project/bronze'
SILVER = '/dbfs/mnt/lab/restricted/ELM-Project/silver'
GOLD = '/dbfs/mnt/lab/restricted/ELM-Project/gold'


def dl_eods(dataset):  # TODO
    '''Defra Earth Observation Data Service
    https://earthobs.defra.gov.uk/
    '''
    uri, name = dataset['uri'], dataset['name']
    f_raw = f'{RAW}/{name}.tif'
    raise NotImplementedError
    LOG.info(f'Task EODS complete: {name}, {uri}, {f_raw}')
    return f_raw

def dl_esri(dataset):
    '''ESRI
    Uses esridump to serially download batches.
    '''
    uri, name = dataset['uri'], dataset['name']
    f_raw = f'{RAW}/{name}.geojson'
    sh_run(f"esri2geojson '{uri}' '{f_raw}'")
    LOG.info(f'Task ESRI complete: {name}, {uri}, {f_raw}')
    return f_raw

def dl_os(dataset):
    '''Ordnance Survey Data Hub
    '''
    uri, name = dataset['uri'], dataset['name']
    f_raw = f'{RAW}/os_ngd/'
    os.mkdirs(f_raw, exist_ok=True)
    osdatahub.DataPackageDownload(os.environ['OS_KEY'], dataset['product_id']).download(dataset['version_id'], output_dir=f_raw)
    LOG.info(f'Task OSM complete: {name}, {uri}, {f_raw}')
    return f_raw

def dl_osm_pbf(dataset):
    '''OpenStreetMap
    '''
    uri, name = dataset['uri'], dataset['name']
    f_raw = f'{RAW}/{name}.osm.pbf'
    sh_run(f'wget {uri} -O {f_raw}')
    LOG.info(f'Task OMS PBF complete: {name}, {uri}, {f_raw}')
    return f_raw

def dl_spol(dataset):  # TODO
    '''SharePoint OnLine
    '''
    uri, name = dataset['uri'], dataset['name']
    f_raw = f'{RAW}/{name}'
    raise NotImplementedError
    LOG.info(f'Task SharePoint complete: {name}, {uri}, {f_raw}')
    return f_raw

def to_gpq(sdf, f): # TODO: metadata
    metadata = {}
    sdf.write.option(metadata).parquet(dbfs(f, True), partitionBy=sdf['geometry.index_id'])

def ingest(dataset, f_raw:str=None):  # TODO: update catalogue
    name = dataset['name']
    srid = dataset['srid'] or 27700
    f_raw = f_raw or dataset['uri']
    f_tmp = f'{RAW}/{name}.parquet'
    f_out = f'{BRONZE}/{name}.parquet'

    # Convert
    LOG.info(f'Convert: {f_raw}, {f_tmp}')
    for file in iglob(f'{f_raw}/*') if os.path.isdir(f_raw) else [f_raw]:
        for layer in fiona.listlayers(file):
            name = f"{snake_case(file.split('/')[-1].split('.')[0])}-{snake_case(layer)}"
            sh_run(['''
                PATH=$PATH:/databricks/miniconda/bin
                TMPDIR=/tmp
                PROJ_LIB=/databricks/miniconda/share/proj
                OGR_GEOMETRY_ACCEPT_UNCLOSED_RING=NO

                ogr2ogr -f Parquet $2 $1 $3
            ''', f_raw, f'{f_out}/layer={name}.parquet', layer], shell=False)

    # Partition
    LOG.info(f'Partition: {f_tmp}, {f_out}')
    sdf = (spark.read.parquet(f_tmp)
        .withColumn('fid', F.monotonically_increasing_id())
        .withColumnsRenamed(dataset['columns'])
        .withColumn('json', mos.grid_tessellateexplode(mos.st_simplify(mos.st_setsrid(mos.st_geomfromwkb('geometry'), F.lit(srid)), F.lit(0.001)), F.lit(3)))
        .drop('geometry')
        .withColumn('is_core', F.col('json.is_core'))
        .withColumn('sindex', F.col('json.index_id'))
        .withColumn('geometry', F.col('json.wkb'))
        .drop('json')
        .transform(to_gpq, f_out)
    )

    # Update Catalogue
    pass

    return sdf



# COMMAND ----------

import json
import os
import subprocess

from elmo_geo import LOG, register


def json_load(f):
    with open(f, "r") as fp:
        obj = json.loads(fp.read())
    return obj

def json_dump(obj, f):
    with open(f, "w", encoding="utf-8") as fp:
        json.dump(obj, fp, ensure_ascii=False, indent=4)

def convert_file(f_in: str, f_out: str):
    out = subprocess.run(["./elmo_geo/io/ogr2gpq.sh", f_in, f_out], shell=False, capture_output=True, text=True)
    LOG.info(out.__repr__())
    return out


register()
catalogue = json_load("./data/catalogue.json")

# COMMAND ----------

for i, dataset in enumerate(catalogue):
    f_in = dataset['uri']
    f_out = f'/dbfs/mnt/lab/restricted/ELM-Project/stg/{dataset["name"]}'
    if (
        dataset['tasks']['convert'] == 'todo'
        and f_in.startswith('/dbfs/mnt/')
        and not os.path.exists(f_out)
    ):
        convert_file(f_in, f_out)
        dataset['tasks']['convert'] = 'done'
        catalogue[i] = dataset

json_dump(catalogue, "./data/catalogue.json")

# COMMAND ----------

# MAGIC %sh
# MAGIC /dbfs/mnt/lab/restricted/ELM-Project/stg/

# COMMAND ----------

import fiona
import os
import re


def gen_files(folder):
    for root, dirs, files in os.walk(folder):
        for file in files:
            yield os.path.join(root, file)


def string_to_dict(string: str, pattern: str) -> dict:
    """Reverse f-string
    https://stackoverflow.com/a/36838374/10450752
    """
    regex = re.sub(r'{(.+?)}', r'(?P<_\1>.+)', pattern)
    return dict(zip(
        re.findall(r'{(.+?)}', pattern),
        list(re.search(regex, string).groups()),
    ))


pattern = '/dbfs/mnt/base/unrestricted/source_{source}/dataset_{dataset}/format_{format}/SNAPSHOT_{version}/{file}\.{ext}'
exts, skipped_exts = ['csv', 'gpkg', 'shp', 'gml', 'gdb', 'geojson', 'tif', 'parquet'], []

for f in gen_files('/dbfs/mnt/base/unrestricted/source_forestry_commission_open_data/'):
    if '/SNAPSHOT_' in f:
        print(f)
        source, dataset, format, version, file, ext = string_to_dict(f, pattern).values()
        format = format.replace(f"_{dataset}", "")
        version = version.replace(f"_{dataset}", "")
        if ext in exts:
            break
        else:
            skipped_exts.append(ext)

skipped_exts

# COMMAND ----------

# MAGIC %ls /dbfs/mnt/base/unrestricted/source_uk_ceh_environmental_info_data_centre/dataset_inventory_of_uk_reservoirs/format_parquet_inventory_of_uk_reservoirs/LATEST_inventory_of_uk_reservoirs

# COMMAND ----------


