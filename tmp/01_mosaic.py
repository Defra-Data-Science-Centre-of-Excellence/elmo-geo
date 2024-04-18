# Databricks notebook source
# MAGIC %pip install -q rich databricks-mosaic

# COMMAND ----------
# elmo_geo.utils.register
import os
import mosaic as mos

from elmo_geo.utils.dbr import spark, dbutils
from elmo_geo.utils.log import LOG
from elmo_geo.utils.types import SparkSession

try:
    from sedona.spark import SedonaContext

    register_sedona = SedonaContext.create
except Exception:
    from sedona.register import SedonaRegistrator

    register_sedona = SedonaRegistrator.registerAll


def register_dir(dir: str):
    """Move up the current working directory to <dir>.
    This is useful for notebooks in subfolders, as they _sometimes_ can't read local modules.
    """
    cwd = os.getcwd()
    nwd = cwd.split(dir)[0] + dir
    if cwd != nwd:
        os.chdir(nwd)
        LOG.info(f"Changed Directory: {cwd} => {nwd}")

def register_mosaic(spark, dbutils):
    mos.enable_mosaic(spark, dbutils)
    mos.enable_gdal(spark)
    LOG.info("Registered:  Mosaic")

def register(spark: SparkSession = spark, dbutils = dbutils, dir: str = "/elmo-geo"):
    register_dir(dir)
    # register_mosaic(spark, dbutils)
    register_sedona(spark)
    LOG.info("Registered: Sedona")
    return True


# COMMAND ----------

from pyspark.sql import functions as F, types as T
import mosaic as mos

from elmo_geo.datasets.catalogue import run_task_on_catalogue
from elmo_geo.utils.misc import dbfs, sh_run


mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

bronze = 'dbfs:/mnt/lab/restricted/ELM-Project/bronze'


def fetch(dataset):
    """Manually fetch dataset from remote location, adding it to bronze.
    Methods: esri, s3, sharepoint, requests
    """
    f_in = dataset["uri"]
    f_out = f"{bronze}/{dataset['name']}"
    if f_in.startswith("https://services"):
        raise NotImplementedError(f"esridump: {dataset}")
    elif f_in.startswith("s3://"):
        raise NotImplementedError(f"s3: {dataset}")
    elif f_in.startswith("spol://"):
        raise NotImplementedError(f"spol: {dataset}")
    else:
        raise NotImplementedError(f"requests: {dataset}")
    dataset['tasks']['fetch'] = f_out
    return dataset


run_task_on_catalogue('fetch', fetch)

# COMMAND ----------

dataset_parcel = {
    'uri': 'dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-parcel-adas.parquet',
    'name': 'rpa-parcel-adas',
    'tasks': {
        'convert': 'todo'
    }
}
convert(dataset_parcel)

# COMMAND ----------

silver = 'dbfs:/mnt/lab/restricted/ELM-Project/silver'


def ogr2gpq(f_in, f_out):
    sh_run(["./elmo_geo/io/ogr2gpq.sh", f_in, f_out], shell=False)
    return f_out

def find_geometry_column(sdf):
    for col in sdf.schema:
        if isinstance(col.dataType, T.BinaryType):
            return col.name

def st_ingest(col:str, srid:int, to_srid:int=27700, precision:float=0.001, resolution:int=3):
    '''Ingest geometries using mosaic with chipping
    '''
    g = F.col(col)
    g = mos.st_geomfromwkb(g)
    g = mos.st_setsrid(g, F.lit(srid))
    g = mos.st_transform(g, to_srid)
    g = mos.st_simplify(g, F.lit(precision))
    g = mos.grid_tessellateexplode(g, F.lit(resolution))
    return g.alias('json')

def convert(dataset):
    '''Convert data from DASH managed or 
    '''
    f_in = getattr(dataset['tasks'], 'fetch', dataset['uri'])
    f_tmp = f"{dataset['name']}.parquet"
    f_out = f"{silver}/{dataset['name']}.parquet"

    if f_in.endswith('.parquet'):
        f_tmp = f_in
    else:
        ogr2gpq(f_in, f_tmp)
    sdf = spark.read.parquet(f_tmp)

    geometry_column = getattr(dataset, 'col', find_geometry_column(sdf))
    other_columns = [col for col in sdf.columns if col!=geometry_column]
    srid = getattr(dataset, 'srid', 27700)
    precision = getattr(dataset, 'precision', 0.001)
    resolution = getattr(dataset, 'resolution', 3)

    (sdf
        .select(
            F.monotonically_increasing_id().alias('fid'),
            *other_columns,
            F.col(geometry_column).alias('geometry'),
        )
        .withColumn('json', st_ingest(col='geometry', srid=srid, precision=precision, resolution=resolution))
        .withColumn({'is_core': 'json.is_core', 'geometry': 'json.wkb', 'sindex': 'json.index_id'})
        .drop('json')
    )
    sdf.write.parquet(dbfs(f_out, True), partitionBy='sindex')
    return dataset


# run_task_on_catalogue('convert', convert)