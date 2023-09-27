try:
    from databricks.sdk.runtime import dbutils, display, sc, spark
except:
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
