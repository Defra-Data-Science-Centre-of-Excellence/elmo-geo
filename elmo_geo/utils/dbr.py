try:
    from databricks.sdk.runtime import dbutils, display, spark  # noqa: F401
except Exception:
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    dbutils = DBUtils(spark)
