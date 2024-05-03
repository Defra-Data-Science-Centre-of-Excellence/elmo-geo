# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest
# MAGIC Create vector datasets that can be easily loaded in spark, and such easily analyse large data.
# MAGIC
# MAGIC ## Format
# MAGIC (Geo)Parquet is the best option, however we must be cautious to save with encoding=WKB as Sedona's `sdf.write.format('geoparquet')` seems unstable between versions, potentially saving as a UDT rather than WKB+metadata as defined in (Geo)Parquet's specification: https://geoparquet.org/.  
# MAGIC Unfortunately, with Sedona we will need the extra steps of, ST_GeomFromWKB and ST_AsBinary, for loading and writing datasets.
# MAGIC We use [BNG](https://www.ordnancesurvey.co.uk/documents/resources/guide-to-nationalgrid.pdf) ([map](https://britishnationalgrid.uk/)) instead of [GeoHash](https://geohash.softeng.co/gc), [H3](https://h3geo.org/), or [S2](https://s2geometry.io/), because most data comes in this form and 1 unit = 1 meter.
# MAGIC
# MAGIC ## Method
# MAGIC 1.  Convert any vector format to (Geo)Parquet with `ogr2ogr`.
# MAGIC 2.  Tidy the geometry by converting to CRS=EPSG:27700, reducing Precision to 1mm, and forcing 2D.
# MAGIC 3.  Partition this data, by adding an identifier (`fid`) chipping and saving according to the spatial index (`sindex`).
# MAGIC 4.  Store the dataset in silver and recording the process in catalogue.
# MAGIC

# COMMAND ----------

import os
import subprocess
from datetime import datetime
from glob import iglob

from fiona import listlayers
from fiona.errors import DriverError
from pyspark.sql import functions as F

from elmo_geo import LOG, register
from elmo_geo.io.file import to_parquet
from elmo_geo.datasets.catalogue import find_datasets, run_task_on_catalogue
from elmo_geo.st.geometry import load_geometry
from elmo_geo.st.index import sindex
from elmo_geo.utils.dbr import spark
from elmo_geo.utils.misc import dbfs, snake_case
from elmo_geo.utils.settings import SILVER

register()

# COMMAND ----------

def list_layers(f: str) -> list[str]:
    try:
        layers = listlayers(f)
    except DriverError:
        layers = []
    return layers


def list_files(f: str) -> list[str]:
    for f1 in iglob(f + "**", recursive=True):
        if os.path.isfile(f1):
            yield f1


def get_to_convert(f: str) -> list[tuple[str, str, str]]:
    if os.path.isfile(f):
        for layer in list_layers(f):
            name = f"layer={snake_case(layer)}"
            yield f, name, layer
    else:
        f = f if f.endswith("/") else f + "/"
        for f1 in list_files(f):
            for layer in list_layers(f1):
                name = f"file={snake_case(f1.replace(f, '').split('.')[0])}/layer={snake_case(layer)}"
                yield f1, name, layer


def ogr_to_geoparquet(f_in, f_out, layer):
    os.makedirs("/".join(f_out.split("/")[:-1]), exist_ok=True)
    out = subprocess.run(
        f"""
        export CONDA_DIR=/databricks/miniconda
        export TMPDIR=/tmp
        export OGR_GEOMETRY_ACCEPT_UNCLOSED_RING=NO
        export PROJ_LIB=$CONDA_DIR/share/proj
        $CONDA_DIR/bin/ogr2ogr -t_srs EPSG:27700 -f Parquet {f_out} {f_in} {layer}
    """,
        capture_output=True,
        text=True,
        shell=True,
    )
    LOG.info(out.__repr__())
    return out

def convert_dataset(f_in, f_out):
    for f0, part, layer in get_to_convert(f_in):
        f1 = f"{f_out}/{part}"
        LOG.info(f"Converting: {f1}")
        ogr_to_geoparquet(f0, f1, layer)

def partition_geoparquet(f_in, f_out, columns):
    return (
        spark.read.parquet(dbfs(f_in, True))
        .withColumn("fid", F.monotonically_increasing_id())
        .withColumnsRenamed(columns)
        .withColumn("geometry", load_geometry())
        .withColumn("geometry", F.expr("EXPLODE(ST_Dump(geometry))"))
        .transform(sindex, method="BNG", resolution="10km", index_fn="chipped_index")
        .transform(to_parquet, f_out)
    )

def convert(dataset):
    name = dataset["name"]
    columns = dataset.get("columns", {})  # rename columns, but don't drop any

    f_raw = dataset.get("bronze", dataset["uri"])
    f_tmp = f"/dbfs/tmp/{name}.parquet"
    f_out = f"{SILVER}/{name}.parquet"

    # Download
    if not f_raw.startswith("/dbfs/"):
        raise TypeError(f"Expecting /dbfs/ dataset: {f_raw}")

    # Convert
    if f_raw.endswith(".parquet"):
        f_tmp = f_raw
    else:
        convert_dataset(f_raw, f_tmp)

    # Partition
    LOG.info(f"Partition: {f_tmp}, {f_out}")
    partition_geoparquet(f_tmp, f_out, columns)

    dataset["bronze"] = dataset["uri"]
    dataset["silver"] = f_out
    dataset["tasks"]["convert"] = datetime.today().strftime("%Y_%m_%d")
    return dataset


# COMMAND ----------

dataset_parcel = {
    "name": "rpa-parcel-adas",
    "bronze": "/dbfs/mnt/lab/restricted/ELM-Project/bronze/rpa-parcel-adas.parquet",
    "columns": {
        "RLR_RW_REFERENCE_PARCELS_DEC_21_LPIS_REF": "id_parcel",
        "Shape": "geometry"
    },
    "tasks": {
        "convert": "todo"
    }
}

convert(dataset_parcel)


dataset_hedge = {
    "name": "rpa-hedge-adas",
    "bronze": "/dbfs/mnt/lab/restricted/ELM-Project/bronze/rpa-hedge-adas.parquet",
    "columns": {
        "geom": "geometry"
    },
    "tasks": {
        "convert": "todo"
    }
}
convert(dataset_hedge)


dataset_osm = find_datasets("osm")[0]
dataset_osm["bronze"] = "/dbfs/tmp/osm-united_kingdom-2024_04_25.parquet"
convert(dataset_osm)

# COMMAND ----------

# run_task_on_catalogue(convert, 'convert')
