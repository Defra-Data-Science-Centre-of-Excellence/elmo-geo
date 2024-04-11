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

f1 = '/Workspace/Repos/andrew.west@defra.gov.uk/init-scripts/DASH.sh'
f2 = '/Volumes/prd_dash_lab/elm_project_restricted/ods/init_script.sh'
dbutils.fs.ls('dbfs:/Workspace/Repos/andrew.west@defra.gov.uk/init-scripts/')
# dbutils.fs.cp(f1, f2)

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


register()
catalogue = json.loads(open("data/catalogue.json").read())


# COMMAND ----------

dl_tasks = {
  'eods': dl_eods,
  'esri': dl_esri,
  'os': dl_os,
  'osm_pbf': dl_osm_pbf,
  'spol': dl_spol,
}

for dataset in catalogue:
    for task, dl in dl_tasks.items():
      if dataset['tasks'][task] == 'todo':
        sdf, dataset = ingest(dataset, dl(dataset))

