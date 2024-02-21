try:
    from databricks.sdk.runtime import dbutils, spark
except Exception:
    ImportWarning("noop dbr")
    from databricks.sdk.core import Config, credentials_provider
    from databricks.sdk.dbutils import RemoteDbUtils
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    @credentials_provider("noop", [])
    def noop_credentials(_: any):
        return lambda: {}

    dbutils = RemoteDbUtils(
        Config(
            host="http://localhost",
            cluster_id="x",
            credentials_provider=noop_credentials,
        ),
    )
