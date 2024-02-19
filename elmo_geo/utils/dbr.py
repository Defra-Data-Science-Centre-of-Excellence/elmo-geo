try:
    from databricks.sdk.runtime import dbutils, display, displayHTML, spark  # noqa:F401
except ImportError or ModuleNotFoundError:
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
    except ImportError or ModuleNotFoundError:
        spark, dbutils = None, None
        ImportWarning("spark and dbutils are unavailable.")
