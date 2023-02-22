from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.column import Column as SparkSeries
from geopandas import GeoDataFrame

from geopandas import read_file
from pyspark.sql import functions as F

from sedona.register import SedonaRegistrator
SedonaRegistrator.registerAll(spark)


def st_fromwkb(col:str='geometry', force2d:bool=False, from_crs:int=None, to_crs:int=None, precision:float=None, simplify:float=None) -> SparkSeries:
  '''Read and clean WKB (binary type) into Sedona (udt type)
  '''
  null = 'ST_GeomFromText("Point EMPTY")'
  geom = f'CASE WHEN ({col} IS NULL) THEN {null} ELSE ST_MakeValid(ST_GeomFromWKB(HEX({col})) END'
  if force2d:
    geom = f'ST_Force_2D({geom})'
  if from_crs is not None and to_crs is not None:
    geom = f'ST_Transform({geom}, "EPSG:{from_crs}", "EPSG:{to_crs}")'
  if precision is not None:
    geom = f'ST_PrecisionReduce({geom}, {precision})'
    simplify = max(simplify, 10**-precision)
    geom = f'ST_SimplifyPreserveTopology({geom}, {simplify})'
  elif simplify is not None:
    geom = f'ST_SimplifyPreserveTopology({geom}, {simplify})'
  return F.expr(geom)


def st_fromgdf(gdf:GeoDataFrame, col:str='geometry', **kwargs) -> SparkDataFrame:
  '''Convert GeoDataFrame to SedonaDataFrame
  '''
  kwargs.update({'from_crs':gdf.crs})
  return (gdf
    .to_wkb()
    .pipe(spark.createDataFrame)
    .withColumn(col, st_load(col, **kwargs))
  )


def ingest(path_in:str, path_out:str, **kwargs) -> SparkDataFrame:
  '''Read any GDAL readable GIS dataframe, and save it as parquet for fast future access.
  '''
  kwargs.update({
    'force2d': True,
    'from_crs': None,
    'to_crs': 27700,
    'precision': 3,
    'simplify': None,
  })
  df = (read_file(path_in, **kwargs)
    .pipe(st_fromgdf)
  )
  df.write.format('geoparquet').save(path_out)
  return df
