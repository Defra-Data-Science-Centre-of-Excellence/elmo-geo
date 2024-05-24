# Databricks notebook source
# MAGIC %md
# MAGIC # OSM Tags
# MAGIC Each OSM feature has several tags.
# MAGIC In this analysis the tags are separated, and the keys are counted.
# MAGIC This is to find the most important tag keys.

# COMMAND ----------

import pandas as pd

from elmo_geo import register

register()

# COMMAND ----------


def extract_php_keys(iterator):
    pattern = r"\"?([\w:]+)\"?(?=\s*=>)"
    for pdf in iterator:
        yield pd.DataFrame({"key": pdf["tags"].str.extractall(pattern)[0]})


# sf = dbfs(find_datasets("osm-united_kingdom")[-1]["silver"])
sf = "dbfs:/tmp/osm-united_kingdom-2024_04_25.parquet"

df = (
    spark.read.parquet(sf)
    .selectExpr(
        """SUBSTRING(CONCAT(
        NVL(CONCAT(",highway=>", highway), ""),
        NVL(CONCAT(",waterway=>", waterway), ""),
        NVL(CONCAT(",aerialway=>", aerialway), ""),
        NVL(CONCAT(",barrier=>", barrier), ""),
        NVL(CONCAT(",man_made=>", man_made), ""),
        NVL(CONCAT(",railway=>", railway), ""),
        NVL(CONCAT(",", other_tags), "")
    ), 2) AS tags"""
    )
    .mapInPandas(extract_php_keys, "key string")
    .groupby("key")
    .count()
    .toPandas()
    .sort_values("count", ascending=False)
)


display(df)

# COMMAND ----------

display(df.query("key.str.contains('water')"))
