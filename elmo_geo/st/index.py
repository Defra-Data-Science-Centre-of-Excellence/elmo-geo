from .settings import BATCHSIZE
from .types import *
from . import join
from pyspark.sql import functions as F



def get_bng_resolution(n:int, /, target:int) -> str:
	target = n // target
	for resolution, cells in {
		'100km': 91,
		# '50km': 364,
		# '20km': 2275,
		'10km': 9100,
		# '5km': 36400,
		'1km': 910000,
	}:
		if target < cells:
			break
	return resolution

def get_bng_grid(resolution:str, spark:SparkSession=None) -> SparkDataFrame:
	sf = 'dbfs:/mnt/lab/unrestricted/elm/ods/os/bng_grid_{resolution}/2023.parquet'
	return spark.read.parquet(sf)


def get_grid(method:str, resolution:Union[str, int]) -> SparkDataFrame:
	if method == 'BNG':
		return get_bng_grid(resolution=resolution)
	elif method == 'GeoHash':
		raise NotImplementedError(method)
	elif method == 'H3':
		raise NotImplementedError(method)
	elif method == 'S2':
		raise NotImplementedError(method)
	else:
		methods = ['BNG', 'GeoHash', 'H3', 'S2']
		raise UserError(f'{method} not in {methods}')

def multi_index(sdf:SparkDataFrame, /, grid:SparkDataFrame) -> SparkDataFrame:
	return (sdf
		.transform(join, grid, lsuffix='')
		.drop('geometry_right')
	)

def single_index(sdf:SparkDataFrame, /, grid:SparkDataFrame) -> SparkDataFrame:
	# there is the chance a centroid is on a grid line
	return (sdf
	  .withColumnRenamed('geometry', 'geometry_feature')
	  .withColumn('geometry', F.expr('ST_Centroid(geometry_feature)'))
		.transform(join, grid, lsuffix='')
		.drop('geometry', 'geometry_right')
	  .withColumnRenamed('geometry_feature', 'geometry')
	)

def chipped_index(sdf:SparkDataFrame, /, grid:SparkDataFrame) -> SparkDataFrame:
	return (sdf
	  .transform(join, grid, lsuffix='')
		.withColumn('geometry', F.expr('ST_Intersection(geometry, geometry_right)'))
		.drop('geometry_right')
	)


def index0(sdf):
	method = 'BNG'
	resolution = get_bng_resolution(sdf.count(), target=BATCHSIZE)
	grid = get_grid(method=method, resolution=resolution)
	return single_index(sdf, grid=grid)

def index1(sdf):
	sf_grid = 'dbfs:/mnt/lab/restricted/ELM-Project/os/bng_grid_1km_with_geohash/version.parquet'
	grid = spark.read.parquet(sf_grid)
	return chipped_index(sdf, grid)


def index(sdf):
  pass
