# Databricks notebook source
# MAGIC %pip install 'git+https://github.com/aw-west-defra/elmo-deps.git' --disable-pip-version-check

# COMMAND ----------

import requests
import geopandas as gpd

from sedona.spark import SedonaContext
SedonaContext.create(spark)

# def test_good_ssl():
#     """This test should pass even if no_ssl_verification fails"""
#     url = "https://sha256.badssl.com/"
#     requests.get(url).ok
# # test_good_ssl()


# dbutils.fs.ls('/Volumes/prd_dash_lab/fcp_unrestricted/config/')

# COMMAND ----------

import mosaic as mos
from pyspark.sql import functions as F, types as T


def st_ingest(col:str='geometry', srid:int=27700, precision:float=0.001, resolution:int=3):
    '''Ingest geometries using mosaic with chipping
    '''
    g = mos.st_geomfromwkb(col)
    g = mos.st_setsrid(g, F.lit(srid))
    g = mos.st_simplify(g, F.lit(precision))
    g = mos.grid_tessellateexplode(g, F.lit(resolution))
    return g.alias('geometry')

def to_gpq(sdf, f): #TODO:metadata
    metadata = {}
    sdf.write.option(metadata).parquet(dbfs(f, True), partitionBy=sdf['geometry.index_id'])

def ingest(dataset):
    uri = dataset['uri']
    name = dataset['name']
    fn = dataset['fn']
    srid = dataset['srid'] or 27700
    f_tmp = f'{FOLDER_RAW}/{name}.parquet'
    f_out = f'{FOLDER_STG}/{name}.parquet'

    # Convert
    LOG.info(f'Convert: {f_raw}, {f_tmp}')
    if os.path.isfile(f_raw):
        sh_run(['./elmo_geo/io/ogr2gpq.sh', f_raw, f_tmp], shell=False)
    else:
        for glob in iglob(f'{f_raw}/*')

    # Partition
    LOG.info(f'Partition: {f_tmp}, {f_out}')
    sdf = spark.read.parquet(f_tmp)
    sdf = (spark.read
        .parquet('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-parcel-adas.parquet')
        .withColumn('fid', F.monotonically_increasing_id())
        .withColumnsRenamed(dataset['columns'])
        .withColumn('json', st_ingest(srid=srid))
        .select(
            'fid',
            *[col for col in columns.values() if col!='geometry'],
            F.col('json.is_core').alias('is_core'),
            F.col('json.index_id').alias('sindex'),
            F.col('json.wkb').alias('geometry'),
        )
        .transform(to_gpq, f_out)
    )
    dataset['filepath'] = f_out
    dataset['fetch'] = DATE
    return sdf, dataset

def suffix(l, r, lsuffix, rsuffix):
    """Add a suffix for columns with the same name in both SparkDataFrames
    """
    for col in l.columns:
        if col in r.columns:
            l.withColumnRenamed(col, col+lsuffix)
            r.withColumnRenamed(col, col+rsuffix)
    return l, r

def st_join(l, r):
    """Spatially join using mosaic's index_id, 
    """
    l, r = suffix(l, r, '_left', '_right')
    return (l.join(r,
            on = l["geometry_left.index_id"] == r["geometry_right.index_id"]
        ).where(
            F.col('geometry_left.is_core') | F.col('geometry_right.is_core')
            | mos.st_contains('geometry_left.wkb', 'geometry_right.wkb')
        )
    )


spark.conf.set("spark.databricks.labs.mosaic.index.system", "BNG")
mos.enable_mosaic(spark, dbutils)

# COMMAND ----------


columns = {
    'RLR_RW_REFERENCE_PARCELS_DEC_21_LPIS_REF': 'id_parcel',
    'Shape': 'geometry',
}

sdf = (spark.read
    .parquet('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-parcel-adas.parquet')
    .withColumn('fid', F.monotonically_increasing_id())
    .withColumnsRenamed(columns)
    .withColumn('json', st_ingest())
    .select(
        'fid',
        *[col for col in columns.values() if col!='geometry'],
        F.col('json.is_core').alias('is_core'),
        F.col('json.index_id').alias('sindex'),
        F.col('json.wkb').alias('geometry'),
    )
)


sdf.display()

# COMMAND ----------

sdf.write.parquet('dbfs:/FileStore/tmp.parquet', partitionBy=F.col('geometry.index_id'))