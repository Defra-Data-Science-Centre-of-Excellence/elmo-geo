from elm_se.variables import CONDA_FOLDER
from elm_se.types import SparkDataFrame
from elm_se.utils import run


def convert_file(f_in, f_out, layer):
	run('/databricks/minconda/bin/ogr2ogr -progress -t_srs EPSG:27700 {f_out} {f_in} {layer}')


def to_gpq1(sdf:SparkDataFrame, sf_out:str):
	# Index Partitioned
	(sdf
		.transform(single_index, method='BNG', resolution=get_bng_resolution(sdf.count()))
		.write.format('geoparquet')
		.save(sf_out, partitionBy='sindex')
	)

def to_gpq2(sdf, sf_out):
	# Not Z-Ordered
	(sdf
		.transform(single_index, method='BNG', resolution='1km')
		.sort('bng')
		.write.format('geoparquet')
		.save(sf_out)
	)

def to_gpq3(sdf, sf_out):
	# Z-Ordered
	(sdf
		.transform(chipped_index, method='BNG', resolution='1km')
		.sort('geohash')
		.write.format('geoparquet')
		.save(sf_out)
	)


# Default to Variable BNG Index Partitioned
to_gpq = to_gpq1
