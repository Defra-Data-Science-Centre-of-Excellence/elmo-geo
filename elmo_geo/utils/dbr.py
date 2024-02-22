try:
    from databricks.sdk.runtime import dbutils, display, displayHTML, spark  # noqa:F401
except Exception:
    ImportWarning("noop dbr")
    from databricks.sdk.dbutils import RemoteDbUtils
    from pyspark.sql.session import SparkSession

    spark, dbutils = SparkSession, RemoteDbUtils
    display, displayHTML = callable, callable
