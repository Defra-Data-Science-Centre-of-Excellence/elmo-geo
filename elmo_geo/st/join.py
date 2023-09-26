# from .log import LOG  # Not Used
from .types import SparkDataFrame
from .io import load_missing
from pyspark.sql import functions as F
from sedona.core.spatialOperator import JoinQuery
from sedona.utils.adapter import Adapter



def sjoin_with_tree(
  sdf_left: SparkDataFrame,
  sdf_right: SparkDataFrame,
	partitioning: str = 'KDBTREE',
	num_partitions: int = 200,
	partition_right: bool = False,
	index_type: str = 'RTREE',
	use_index: bool = True,
	consider_boundary_itersection: bool = True,
) -> SparkDataFrame:
	# RDD
	rdd_left = Adapter.toSpatialRdd(sdf_left, 'geometry')
	rdd_right = Adapter.toSpatialRdd(sdf_right, 'geometry')
	rdd_left.analyze()
	rdd_right.analyze()
	# Build Tree
	if partition_right:
		rdd_right.spatialPartitioning(partitioning=partitioning, num_partitions=num_partitions)
		rdd_left.spatialPartitioning(rdd_right.getPartitioner())
	else:
		rdd_left.spatialPartitioning(partitioning=partitioning, num_partitions=num_partitions)
		rdd_right.spatialPartitioning(rdd_left.getPartitioner())
	if use_index:
		rdd_left.buildIndex(indexType=index_type, buildIndexOnSpatialPartitionedRDD=True)
	# Join
	rdd = JoinQuery.SpatialJoinQueryFlat(rdd_right, rdd_left,
		useIndex = use_index,
		considerBoundaryIntersection = consider_boundary_itersection,
	)
	return Adapter.toDf(spatialPairRDD=rdd, sparkSession=spark)



def sjoin(left:SparkDataFrame, right:SparkDataFrame, distance:float=0) -> SparkDataFrame:
	'''Spatial Join, only returning keys
	Only suitable for minimal SparkDataFrames
	left.select('id_left', 'geometry')
	right.select('id_right', 'geometry')
	'''
	# Distance Join
	if 0 < distance:
		left.withColumn('geometry', F.expr('ST_MakeValid(ST_Buffer(geometry, {distance}))'))
	# Add to SQL
	left.createOrReplaceTempView('left')
	right.createOrReplaceTempView('right')
	# Join
	sdf = spark.sql(f'''
		SELECT id_left, id_right
		FROM left, right
		WHERE ST_Intersects(left.geometry, right.geometry)
	''')
	# Remove from SQL
	spark.sql('DROP TABLE left')
	spark.sql('DROP TABLE right')
	return sdf


def join(sdf_left:SparkDataFrame, sdf_right:SparkDataFrame, how:str='full', lsuffix:str='_left', rsuffix:str='_right', distance:float=0) -> SparkDataFrame:
	'''Spatial Join with how
	how: {full, inner, left, right} 
	'''
	# ID
	sdf_left = sdf_left.withColumn('id_left', F.monotonically_increasing_id())
	sdf_right = sdf_right.withColumn('id_right', F.monotonically_increasing_id())
	# How
  how_outer = ['outer', 'outer_left', 'outer_right', 'anti', 'anti_left', 'anti_right']
  if how in how_outer:
		raise NotImplementedError(how_outer)
	how_left = 'full' if how in ['left', 'full'] else 'inner'
	how_right = 'full' if how in ['right', 'full'] else 'inner'
	# Spatial Join
	sdf =	sjoin(
		sdf_left.select('id_left', 'geometry'),
		sdf_right.select('id_right', 'geometry'),
		distance = distance,
	)
	# Rename
	for col in sdf_left.columns:
		if col in sdf_right.columns:
			sdf_left = sdf_left.withColumnRenamed(col, col+lsuffix)
			sdf_right = sdf_right.withColumnRenamed(col, col+rsuffix)
	geometry_left = f'left.geometry{lsuffix}'
	geometry_right = f'right.geometry{rsuffix}'
	return (sdf
		.join(sdf_left.drop('geometry'), how=how_left, on='id_left')
		.join(sdf_right.drop('geometry'), how=how_right, on='id_right')
		.withColumn('geometry'+lsuffix, load_missing('geometry'+lsuffix))
		.withColumn('geometry'+rsuffix, load_missing('geometry'+rsuffix))
		.drop('id_left', 'id_right')
	)
