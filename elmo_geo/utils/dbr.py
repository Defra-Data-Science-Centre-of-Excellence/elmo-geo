try:
  from databricks.sdk.runtime import (
    spark,
    sc,
    dbutils,
    display,
  )
except:
  from pyspark.dbutils import DBUtils
  from pyspark.sql import SparkSession

  spark = SparkSession.getActiveSession()
  dbutils = DBUtils(spark)
