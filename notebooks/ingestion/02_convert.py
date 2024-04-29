# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest
# MAGIC Create vector datasets that can be easily loaded in spark, and such easily analyse large data.
# MAGIC
# MAGIC ## Format
# MAGIC (Geo)Parquet is the best option, however we must be cautious to save with encoding=WKB as Sedona's `sdf.write.format('geoparquet')` seems unstable between versions, potentially saving as a UDT rather than WKB+metadata as defined in (Geo)Parquet's specification: https://geoparquet.org/.  
# MAGIC Unfortunately, with Sedona we will need the extra steps of, ST_GeomFromWKB and ST_AsBinary, for loading and writing datasets.
# MAGIC
# MAGIC ## Method
# MAGIC 1.  Convert any vector format to (Geo)Parquet with `ogr2ogr`.
# MAGIC 2.  Tidy the geometry by converting to CRS=EPSG:27700, reducing Precision to 1mm, and forcing 2D.
# MAGIC 3.  Partition this data, by adding an identifier (`fid`) chipping and saving according to the spatial index (`sindex`).
# MAGIC 4.  Store the dataset in silver and recording the process in catalogue.
# MAGIC

# COMMAND ----------

import os.path
import subprocess

from datetime import datetime
from fiona import listlayers
from fiona.errors import DriverError
from glob import iglob
from pyspark.sql import functions as F, types as T

from elmo_geo import register, LOG
from elmo_geo.datasets.catalogue import run_task_on_catalogue, find_datasets
from elmo_geo.io.file import to_pq
from elmo_geo.st.geometry import load_geometry
from elmo_geo.st.index import chip
from elmo_geo.utils.misc import dbfs, sh_run, snake_case
# from elmo_geo.utils.settings import BRONZE, SILVER


BRONZE = '/dbfs/mnt/lab/restricted/ELM-Project/bronze'
SILVER = '/dbfs/mnt/lab/restricted/ELM-Project/silver'

register()

# COMMAND ----------

# DBTITLE 1,move to a module
def list_layers(f):
    try:
        layers = listlayers(f)
    except DriverError:
        layers = []
    return layers

def list_files(f):
    for f1 in iglob(f+'**', recursive=True):
        if os.path.isfile(f1):
            yield f1

def get_to_convert(f):
    if not f.endswith(".parquet"):
        if os.path.isfile(f):
            for layer in list_layers(f):
                name = f"layer={snake_case(layer)}"
                yield f, name, layer
        else:
            f = f if f.endswith("/") else f+"/"
            for f1 in list_files(f):
                for layer in list_layers(f1):
                    name = f"file={snake_case(f1.replace(f, '').split('.')[0])}/layer={snake_case(layer)}"
                    yield f1, name, layer

def convert_file(f_in, f_out, layer):
    out = subprocess.run(['/databricks/miniconda/bin/ogr2ogr -f Parquet', f_out, f_in, layer], capture_output=True, text=True, shell=True)
    LOG.info(out.__repr__())
    return out

def convert(dataset):
    name = dataset['name']
    crs = dataset.get('crs', 'EPSG:27700')  # Coordinate Reference System
    precision = dataset.get('precision', 0.001)  # Precision at 1mm
    resolution = dataset.get('resolution', 3)  # 3=1km BNG grid
    columns = dataset.get('columns', {})  # rename columns, but don't drop any

    f_raw = dataset.get('bronze', dataset['uri'])
    f_tmp = f'{BRONZE}/{name}.parquet'
    f_out = f'{SILVER}/{name}.parquet'

    # Download
    if not f_raw.startswith('/dbfs/'):
        raise TypeError(f'Expecting /dbfs/ dataset: {f_raw}')

    # Convert
    for f0, name, layer in get_to_convert(f_raw):
        f1 = f"{f_out}/{name}"
        LOG.info(f"Convert File: {f0}, {f1}, {layer}")
        convert_file(f0, f1, layer)

    # Partition
    LOG.info(f'Partition: {f_tmp}, {f_out}')
    sdf = (spark.read.parquet(dbfs(f_tmp, True))
        .withColumn('fid', F.monotonically_increasing_id())
        .withColumnsRenamed(columns)
        .withColumn('geometry', load_geometry(from_crs=crs))
        .transform(chip)
        .transform(to_pq, f_out)
    )

    dataset['bronze'] = dataset['uri']
    dataset['silver'] = f_out
    dataset['tasks']['convert'] = datetime.today().strftime('%Y_%m_%d')
    return dataset

# COMMAND ----------

# DBTITLE 1,Testing
# dataset_osm = {
#     "name": "osm-united_kingdom-2024_04_25",
#     "uri": "/dbfs/mnt/base/unrestricted/source_openstreetmap/dataset_united_kingdom/format_PBF_united_kingdom/SNAPSHOT_2024_04_25_united_kingdom/united-kingdom-latest.osm.pbf",
#     "columns": {
#     },
#     "tasks": {
#         "convert": "todo"
#     },
#     "distance": 24,
#     "knn": True
# }
# convert(dataset_osm)


dataset_parcel = {
    'uri': '/dbfs/mnt/lab/restricted/ELM-Project/bronze/rpa-parcel-adas.parquet',
    'name': 'rpa-parcel-adas',
    "columns": {
        "Shape": "geometry"
    },
    'tasks': {
        'convert': 'todo'
    }
}
convert(dataset_parcel)


# dataset_hedge = find_datasets('rpa-hedge-adas')[0]
# convert(dataset_hedge)

# COMMAND ----------

# MAGIC %ls /dbfs/mnt/lab/restricted/ELM-Project/*/

# COMMAND ----------

# DBTITLE 1,main
# run_task_on_catalogue(convert, 'convert')
