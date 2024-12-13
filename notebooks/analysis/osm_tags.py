# Databricks notebook source
# MAGIC %md
# MAGIC # OSM Tags
# MAGIC Each OSM feature has several tags.
# MAGIC In this analysis the tags are separated, and the keys are counted.
# MAGIC This is to find the most important tag keys.
# MAGIC
# MAGIC ### Methodology
# MAGIC 1. First merge OSM specified tags ("waterway", "highway", etc.) and "other_tags".
# MAGIC 2. Convert the tags formatting from "key1"=>"value1",key2=>value2 to dictionary.
# MAGIC
# MAGIC
# MAGIC ### Water Tags
# MAGIC
# MAGIC ##### Water Features
# MAGIC There is one specified key, "waterway", considered relevant.
# MAGIC When filtering keys containing the word "water" there is 100s of other keys, those with >1,000 count are included in the table.
# MAGIC Other words like "stream", "lake", and "river" were considered, those with >10,000 count have been included in the table.
# MAGIC Few features have high counts, I will just keep "water" assuming they are "waterbody" features.
# MAGIC
# MAGIC ##### Peatland Features
# MAGIC When searching for ditches, drains, grips, gullies, and hags, there is little data that covers peatland features.
# MAGIC OSM will not be used to identify peatland features.
# MAGIC
# MAGIC | **Key**                                 | **Count** | **To Keep** |
# MAGIC | --------------------------------------- | --------: | ----------- |
# MAGIC | waterway                                |   628,845 | Keep as `watercourse`
# MAGIC | **Potential Water Keys:**
# MAGIC | water                                   |    95,646 | Keep as `waterbody`
# MAGIC | water_source                            |     2,138 | Too few
# MAGIC | generator:output:hot_water              |     1,713 | Too few
# MAGIC | drinking_water                          |     1,621 | Too few
# MAGIC | stream                                  |        31 | Too few
# MAGIC | lake                                    |        14 | Too few
# MAGIC | river                                   |         9 | Too few
# MAGIC | seamark:type                            |    18,326 |
# MAGIC | tidal                                   |    13,524 |
# MAGIC | **Potential Peatland Keys:**
# MAGIC | ditch                                   |        44 | Too few
# MAGIC | drain                                   |        17 | Too few
# MAGIC | electric_fence_grip                     |         4 | Not relevant
# MAGIC
# MAGIC ### TODO: Wall Tags
# MAGIC ### TODO: Heritage Tags
# MAGIC

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
