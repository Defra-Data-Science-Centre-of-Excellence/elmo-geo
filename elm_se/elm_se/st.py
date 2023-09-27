from elm_se.types import *
from pyspark.sql import functions as F
from elm_se.io import load_missing


def bng_index(sdf:SparkDataFrame, resolution:int=3) -> SparkDataFrame:
  '''Attach the index of geometries
  '''
  sdf_grid = spark.read.parquet(f'dbfs:/mnt/lab/restricted/elm_data/os_bng_grid/{resolution}.parquet')
  return sjoin(sdf, sdf_grid, lsuffix='', rsuffix='_grid').drop('geometry_grid')


def sjoin(sdf_left:SparkDataFrame, sdf_right:SparkDataFrame, lsuffix:str='_left', rsuffix:str='_right', distance:float=0, spark=None) -> SparkDataFrame:
  # Rename
  for col in sdf_left.columns:
    if col in sdf_right.columns:
      sdf_left = sdf_left.withColumnRenamed(col, col+lsuffix)
      sdf_right = sdf_right.withColumnRenamed(col, col+rsuffix)
  geometry_left = f'left.geometry{lsuffix}'
  geometry_right = f'right.geometry{rsuffix}'
  # Add to SQL
  sdf_left.createOrReplaceTempView('left')
  sdf_right.createOrReplaceTempView('right')
  # Join
  if distance == 0:
    # Intersects Join
    sdf = spark.sql(f'''
      SELECT left.*, right.*
      FROM left, right
      WHERE ST_Intersects({geometry_left}, {geometry_right})
    ''')
  elif distance > 0:
    # Intersects+Distance Join
    sdf = spark.sql(f'''
      SELECT left.*, right.*
      FROM left, right
      WHERE ST_Intersects(ST_MakeValid(ST_Buffer({geometry_left}, {distance})), {geometry_right})
    ''')
  elif False:
    # Distance Join
    sdf = spark.sql(f'''
      SELECT left.*, right.*
      FROM left, right
      WHERE ST_Distance({geometry_left}, {geometry_right}) < {distance}
    ''')
  else:
    raise TypeError(f'distance should be positive real: {distance}')
  # Remove from SQL
  spark.sql('DROP TABLE left')
  spark.sql('DROP TABLE right')
  return sdf

def join(sdf_left:SparkDataFrame, sdf_right:SparkDataFrame, how:str='inner', lsuffix:str='_left', rsuffix:str='_right', distance:float=0) -> SparkDataFrame:
  '''Spatial Join
  how can be inner/full/left/right, no implementation for outer/outer_left/outer_right/
  '''
  sdf_left = sdf_left.withColumn('.left', F.monotonically_increasing_id())
  sdf_right = sdf_right.withColumn('.right', F.monotonically_increasing_id())
  how_left = 'full' if how in ['left', 'full'] else 'inner'
  how_right = 'full' if how in ['right', 'full'] else 'inner'
  return (
    sjoin(
      sdf_left.select('.left', 'geometry'),
      sdf_right.select('.right', 'geometry'),
      lsuffix=lsuffix, rsuffix=rsuffix, distance=distance,
    )
    .join(sdf_left.drop('geometry'), how=how_left, on='.left')
    .join(sdf_right.drop('geometry'), how=how_right, on='.right')
    .withColumn('geometry'+lsuffix, load_missing('geometry'+lsuffix))
    .withColumn('geometry'+rsuffix, load_missing('geometry'+rsuffix))
    .drop('.left', '.right')
  )
