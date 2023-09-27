from typing import Callable, Union

from geopandas import GeoDataFrame, GeoSeries
from pandas import DataFrame as PandasDataFrame
from pandas import Series as PandasSeries
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.column import Column as SparkSeries
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from sedona.sql.types import GeometryType as SedonaType
from shapely.geometry.base import BaseGeometry

DataFrame = Union[SparkDataFrame, PandasDataFrame, GeoDataFrame]
Series = Union[SparkSeries, PandasSeries, GeoSeries]
Geometry = Union[GeoDataFrame, GeoSeries, BaseGeometry, SedonaType]
