from elmo_geo.utils.types import T, PandasSeries, GeoSeries
from pyspark.sql.functions import pandas_udf
from shapely.ops import transform



@pandas_udf(T.BinaryType())
def remove_z(geoms: PandasSeries) -> PandasSeries:
  """Remove the z coordinate from WKB geometries"""
  return (GeoSeries.from_wkb(geoms)
    .map(lambda g: transform(lambda x, y, z=None: (x, y), g))
    .to_wkb()
  )


@pandas_udf(T.BooleanType())
def has_z(geoms: PandasSeries) -> PandasSeries:
  """Identify whether WKB geometries have a z coordinate"""
  return GeoSeries.from_wkb(geoms).has_z


@pandas_udf(T.BooleanType())
def is_valid(geoms: PandasSeries) -> PandasSeries:
  """Identify whether WKB geometries are valid"""
  return GeoSeries.from_wkb(geoms).is_valid
