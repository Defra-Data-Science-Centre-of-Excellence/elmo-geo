import geopandas as gpd
import pandas as pd
from pyspark.sql.functions import pandas_udf
from shapely.ops import transform


@pandas_udf("binary")
def remove_z(geoms: pd.Series) -> pd.Series:
    """Remove the z coordinate from WKB geometries"""
    return (
        gpd.GeoSeries.from_wkb(geoms)
        .map(lambda g: transform(lambda x, y, z=None: (x, y), g))
        .to_wkb()
    )


@pandas_udf("boolean")
def has_z(geoms: pd.Series) -> pd.Series:
    """Identify whether WKB geometries have a z coordinate"""
    return gpd.GeoSeries.from_wkb(geoms).has_z


@pandas_udf("boolean")
def is_valid(geoms: pd.Series) -> pd.Series:
    """Identify whether WKB geometries are valid"""
    return gpd.GeoSeries.from_wkb(geoms).is_valid



# ST_Dump 
@F.udf(T.ArrayType(T.BinaryType()))
def st_dump_to_list(col):
  gs = gpd.GeoSeries.from_wkb([col])
  return gs.explode().wkb.tolist()

def st_explode(sdf):
  return (sdf
    .withColumn('geometry', F.expr('ST_AsBinary(geometry)')
    .withColumn('geometry', st_dump_to_list('geometry'))
    .withColumn('geometry', F.explode('geometry'))
    .withColumn('geometry', F.expr('ST_GeomFromWKB('geometry'))
  )