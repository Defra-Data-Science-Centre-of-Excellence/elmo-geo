from pyspark.sql import types as T
from typing import Union, Callable
from pyspark.sql import SparkSession

from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.column import Column as SparkSeries
from pandas import DataFrame as PandasDataFrame, Series as PandasSeries
from geopandas import GeoDataFrame, GeoSeries
from shapely.geometry.base import BaseGeometry
from sedona.sql.types import GeometryType as SedonaType


DataFrame = Union[SparkDataFrame, PandasDataFrame, GeoDataFrame]
Series = Union[SparkSeries, PandasSeries, GeoSeries]
Geometry = Union[GeoDataFrame, GeoSeries, BaseGeometry, SedonaType]
