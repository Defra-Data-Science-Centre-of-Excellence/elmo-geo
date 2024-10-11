from typing import Union

import pyarrow as pa
from geopandas import GeoDataFrame, GeoSeries
from pandas import DataFrame as PandasDataFrame
from pandas import Series as PandasSeries
from pyspark.sql import SparkSession  # noqa:F401
from pyspark.sql import types as T  # noqa:F401
from pyspark.sql.column import Column as SparkSeries
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from sedona.sql.types import GeometryType as SedonaType
from shapely.geometry.base import BaseGeometry

DataFrame = Union[SparkDataFrame, PandasDataFrame, GeoDataFrame]
Series = Union[SparkSeries, PandasSeries, GeoSeries]
Geometry = Union[GeoDataFrame, GeoSeries, BaseGeometry, SedonaType]

# Mapping of PyArrow types to PySpark types
type_mapping_arrow_to_spark = {
    pa.string(): T.StringType(),
    pa.int8(): T.IntegerType(),
    pa.int16(): T.IntegerType(),
    pa.int32(): T.IntegerType(),
    pa.int64(): T.IntegerType(),
    pa.float32(): T.FloatType(),
    pa.float64(): T.DoubleType(),
    pa.bool_(): T.BooleanType(),
    pa.date32(): T.DateType(),
    pa.timestamp("s"): T.TimestampType(),
    pa.binary(): T.BinaryType(),
    pa.list_: lambda field: T.ArrayType(arrow_schema_to_spark(field.value_type)),
}

# Mapping of PySpark types to PyArrow types
type_mapping_spark_to_arrow = {
    T.StringType(): pa.string(),
    T.IntegerType(): pa.int32(),
    T.FloatType(): pa.float32(),
    T.DoubleType(): pa.float64(),
    T.BooleanType(): pa.bool_(),
    T.DateType(): pa.date32(),
    T.TimestampType(): pa.timestamp("s"),
    T.BinaryType(): pa.binary(),
    T.ArrayType: lambda field: pa.list_(spark_schema_to_arrow(field.elementType)),
}


def arrow_schema_to_spark(arrow_schema: pa.schema) -> T.StructType:
    """
    Convert a PyArrow schema to a PySpark schema.

    Parameters:
    arrow_schema (pa.Schema): The PyArrow schema to convert.

    Returns:
    StructType: The corresponding PySpark schema.
    """
    spark_fields = []
    for field in arrow_schema:
        spark_type = type_mapping_arrow_to_spark.get(field.type)
        if spark_type is None:
            if isinstance(field.type, pa.ListType):
                spark_type = T.ArrayType(arrow_schema_to_spark(field.type.value_type))
            else:
                raise ValueError(f"Unsupported PyArrow type: {field.type}")
        spark_fields.append(T.StructField(field.name, spark_type, field.nullable))
    return T.StructType(spark_fields)


def spark_schema_to_arrow(spark_schema: T.StructType) -> pa.schema:
    """
    Convert a PySpark schema to a PyArrow schema.

    Parameters:
    spark_schema (StructType): The PySpark schema to convert.

    Returns:
    pa.Schema: The corresponding PyArrow schema.
    """
    arrow_fields = []

    for field in spark_schema.fields:
        arrow_type = type_mapping_spark_to_arrow.get(type(field.dataType))

        if arrow_type is None:
            raise ValueError(f"Unsupported PySpark type: {field.dataType}")

        # If the type is an ArrayType, handle it specifically
        if isinstance(field.dataType, T.ArrayType):
            arrow_type = pa.list_(spark_schema_to_arrow(field.dataType.elementType))

        arrow_fields.append(pa.field(field.name, arrow_type, field.nullable))

    return pa.schema(arrow_fields)
