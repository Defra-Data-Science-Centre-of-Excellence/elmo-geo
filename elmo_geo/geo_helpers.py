from pyspark.sql.functions import pandas_udf
import pandas as pd
import geopandas as gpd
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
