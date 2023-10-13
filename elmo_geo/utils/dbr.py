try:
    from databricks.sdk.runtime import dbutils, spark
except Exception:
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
