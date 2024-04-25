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

def convert(dataset):
    name = dataset['name']
    crs = getattr(dataset, 'crs', 'EPSG:27700')  # Coordinate Reference System
    precision = getattr(dataset, 'precision', 0.001)  # Precision at 1mm
    resolution = getattr(dataset, 'resolution', 3)  # 3=1km BNG grid

    f_raw = getattr(dataset, 'bronze', dataset['uri'])
    f_tmp = f'{BRONZE}/{name}.parquet'
    f_out = f'{SILVER}/{name}.parquet'

    # Download
    if not f_raw.startswith('/dbfs/'):
        raise TypeError(f'Expecting /dbfs/ dataset: {f_raw}')

    # Convert
    if f_raw.endswith('.parquet'):
        f_tmp = f_raw
    else:
        LOG.info(f'Convert: {f_raw}, {f_tmp}')
        if os.path.isfile(f_raw):
            f2
        else:
            for f0 in iglob(f'{f_raw}/*'):
                f1 = f"{f_tmp}/file={snake_case(f0.replace(f_raw, ''))}"
                for layer in listlayers(f0):
                    f2 = f"{f1}/layer={snake_case(layer)}"
                    print(f0, f2, layer)
                    # out = subprocess.run([r'''
                    #     PATH=$PATH:/databricks/miniconda/bin
                    #     TMPDIR=/tmp
                    #     PROJ_LIB=/databricks/miniconda/share/proj
                    #     OGR_GEOMETRY_ACCEPT_UNCLOSED_RING=NO
                    #     echo ogr2ogr -f Parquet $2 $1 $3
                    # ''', f, f'{f_tmp}/layer={name}', layer], capture_output=True, text=True, shell=True)
                    # LOG.info(out.__repr__())

    # # Partition
    # LOG.info(f'Partition: {f_tmp}, {f_out}')
    # sdf = (spark.read.parquet(dbfs(f_tmp, True))
    #     .withColumn('fid', F.monotonically_increasing_id())
    #     .withColumnsRenamed(dataset['columns'])
    #     .withColumn('geometry', load_geometry(crs=crs))
    #     .transform(chip)
    #     .transform(to_pq, f_out)
    # )

    dataset['bronze'] = dataset['uri']
    dataset['silver'] = f_out
    dataset['tasks']['convert'] = datetime.today().strftime('%Y_%m_%d')
    return dataset

# COMMAND ----------

dataset_osm = {
    "name": "osm-united_kingdom-2024_04_25",
    "uri": "/dbfs/mnt/base/unrestricted/source_openstreetmap/dataset_united_kingdom/format_PBF_united_kingdom/SNAPSHOT_2024_04_25_united_kingdom/united-kingdom-latest.osm.pbf",
    "columns": {
    },
    "tasks": {
        "convert": "todo"
    }
}
convert(dataset_osm)


# dataset_parcel = {
#     'uri': '/dbfs/mnt/lab/restricted/ELM-Project/bronze/rpa-parcel-adas.parquet',
#     'name': 'rpa-parcel-adas',
#     'tasks': {
#         'convert': 'todo'
#     }
# }
# convert(dataset_parcel)


# dataset_hedge = find_datasets('rpa-hedge-adas')[0]
# convert(dataset_hedge)

# COMMAND ----------

run_task_on_catalogue(convert, 'convert')
