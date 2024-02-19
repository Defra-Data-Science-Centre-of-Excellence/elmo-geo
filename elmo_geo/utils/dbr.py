try:
    from databricks.sdk.runtime import dbutils, display, displayHTML, spark
except Exception:
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
