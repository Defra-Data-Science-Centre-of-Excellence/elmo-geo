# Databricks notebook source
# MAGIC %pip install -qU git+https://github.com/aw-west-defra/cdap_geo.git

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
# MAGIC import org.apache.spark.sql.types._
# MAGIC object GeoPackageDialect extends JdbcDialect {
# MAGIC   override def canHandle(url: String): Boolean = url.startsWith("jdbc:sqlite")
# MAGIC
# MAGIC   override def getCatalystType(
# MAGIC     sqlType: Int,
# MAGIC     typeName: String,
# MAGIC     size: Int,
# MAGIC     md: MetadataBuilder
# MAGIC   ): Option[DataType] = typeName match {
# MAGIC     case "BOOLEAN" => Some(BooleanType)
# MAGIC     case "TINYINT" => Some(ByteType)
# MAGIC     case "SMALLINT" => Some(ShortType)
# MAGIC     case "MEDIUMINT" => Some(IntegerType)
# MAGIC     case "INT" | "INTEGER" => Some(LongType)
# MAGIC     case "FLOAT" => Some(FloatType)
# MAGIC     case "DOUBLE" | "REAL" => Some(DoubleType)
# MAGIC     case "TEXT" => Some(StringType)
# MAGIC     case "BLOB" => Some(BinaryType)
# MAGIC     case "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT" | "MULTILINESTRING" |
# MAGIC       "MULTIPOLYGON" | "GEOMETRYCOLLECTION" | "CIRCULARSTRING" | "COMPOUNDCURVE" |
# MAGIC       "CURVEPOLYGON" | "MULTICURVE" | "MULTISURFACE" | "CURVE" | "SURFACE" => Some(BinaryType)
# MAGIC     case "DATE" => Some(DateType)
# MAGIC     case "DATETIME" => Some(StringType)
# MAGIC   }
# MAGIC }
# MAGIC JdbcDialects.registerDialect(GeoPackageDialect)

# COMMAND ----------

import mosaic
from cdap_geo.read import read_gpkg
from cdap_geo.sedona import st_load
from pyspark.sql import functions as F
from sedona.register import SedonaRegistrator

mosaic.enable_mosaic(spark, dbutils)
mosaic.enable_gdal(spark)

SedonaRegistrator.registerAll(spark)

# COMMAND ----------

f_rpa_parcels = "/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_reference_parcels/format_GPKG_rpa_reference_parcels/SNAPSHOT_2023_02_07_rpa_reference_parcels/reference_parcels.zip/reference_parcels.gpkg"
f_parcels = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/parcels/2023_02_07.parquet"

# COMMAND ----------

sdf_rpa_parcels = (
    read_gpkg(f_rpa_parcels).select(
        F.concat("SHEET_ID", "PARCEL_ID").alias("id"),
        #     bng('geometry').alias('bng'),
        st_load("geometry").alias("geometry"),
    )
    #   .repartition('bng')
)

display(sdf_rpa_parcels)

# COMMAND ----------


df = spark.read.format("binaryFile").load("dbfs:/tmp/RPA_LandCover.gpkg")
display(df)
