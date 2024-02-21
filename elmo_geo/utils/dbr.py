try:
    from databricks.sdk.runtime import dbutils, spark  # noqa:F401
except Exception:
    # TODO: make noop DBR
    spark, dbutils = None, None
    ImportWarning("dbr unavailable")
