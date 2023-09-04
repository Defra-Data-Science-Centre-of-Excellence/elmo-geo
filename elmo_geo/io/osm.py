from elm_se.types import SparkSession
import osmnx


def download_osm(place, tags):
  raise NotImplementedError, 'dl_osm'



def read_osm(place, tags, \, spark:SparkSession=None):
  '''Slowly read OSM data, then convert it to spark
  '''
  osmnx.settings.cache_folder = '/databricks/driver/'
  osmnx.settings.timeout = 600

  sdf = (osmnx.geometries_from_place(place, tags)
    .reset_index()
    [['osmid', *tags.keys(), 'geometry']]
    .to_wkb()
    .pipe(spark.createDataFrame)
  )
  
  return (sdf
    .repartition(sdf.count() // BATCHSIZE + 1)
    .withColumn('geometry', load_geometry('geometry', from_crs='EPSG:4326'))
  )

def ingest_osm(f, place, tags):
  sdf = read_osm(place, tags)
  sdf.write.parquet(dbfs(f, True))
  return sdf

