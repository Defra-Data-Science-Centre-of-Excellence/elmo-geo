from functools import partial

import geopandas as gpd
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

from elmo_geo.utils.dbr import spark

from .sjoin import sjoin_bbox


# Example
def udf_intersects(pdf, lsuffix: str = "_left", rsuffix: str = "_right", distance: float = 0, **kwargs) -> pd.DataFrame:
    geometry_left = gpd.GeoSeries.from_wkb(pdf[f"geometry{lsuffix}"])
    geometry_right = gpd.GeoSeries.from_wkb(pdf[f"geometry{rsuffix}"])
    intersects = geometry_left.distance(geometry_right) < distance if distance else geometry_left.intersects(geometry_right)
    return pdf.iloc[intersects]


def join_parcel(parcels: object, features: object, schema: str | None = None, udf_fn: callable = udf_intersects, **kwargs) -> SparkDataFrame:
    sdf = sjoin_bbox(parcels.sdf(), features.sdf(), **kwargs)
    if schema is None:
        schema = sdf.limit(10).toPandas().pipe(udf_fn).pipe(spark.createDataFrame).schema
    return sdf.mapInPandas(partial(map, partial(udf_fn, **kwargs)), schema=schema)


# Parcel Proportion
def udf_parcel_proportion(pdf, lsuffix: str = "_left", rsuffix: str = "_right", **kwargs) -> pd.DataFrame:
    geometry_left = gpd.GeoSeries.from_wkb(pdf[f"geometry{lsuffix}"])
    geometry_right = gpd.GeoSeries.from_wkb(pdf[f"geometry{rsuffix}"])

    intersects = geometry_left.intersects(geometry_right)

    pdf = pdf.iloc[intersects]
    geometry_left = geometry_left.iloc[intersects]
    geometry_right = geometry_right.iloc[intersects]

    geometry = geometry_left.intersection(geometry_right)
    pdf["area"] = (geometry.area / geometry_left.area).clip(0, 1)
    return pdf


join_parcel_proportion = partial(join_parcel, udf_fn=udf_parcel_proportion)


# Boundary Proportion
def udf_boundary_proportion(pdf, lsuffix: str = "_left", rsuffix: str = "_right", **kwargs) -> pd.DataFrame:
    geometry_left = gpd.GeoSeries.from_wkb(pdf[f"geometry{lsuffix}"])
    geometry_right = gpd.GeoSeries.from_wkb(pdf[f"geometry{rsuffix}"])

    intersects = geometry_left.intersects(geometry_right)

    pdf = pdf.iloc[intersects]
    geometry_left = geometry_left.iloc[intersects]
    geometry_right = geometry_right.iloc[intersects]

    geometry = geometry_left.intersection(geometry_right)
    pdf["area"] = (geometry.area / geometry_left.area).clip(0, 1)
    return pdf


join_parcel_proportion = partial(join_parcel, udf_fn=udf_boundary_proportion)
