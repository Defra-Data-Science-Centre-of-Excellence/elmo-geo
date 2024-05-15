# Databricks notebook source
# MAGIC %md
# MAGIC # Identify
# MAGIC Identify datasets not in DASH governed data area.
# MAGIC 1.  Fetch them manually and upload them to the bronze area.
# MAGIC 2.  Add their catalogue entry rather than manually to the catalogue.
# MAGIC 3.  Run `add_to_catalogue`, 

# COMMAND ----------

from elmo_geo.datasets.catalogue import add_to_catalogue

# COMMAND ----------

manual_datasets = [
    {
        "name": "rpa-parcel-adas",
        "uri": "missing sharepoint link",
        "bronze": "/dbfs/mnt/lab/restricted/ELM-Project/bronze/rpa-parcel-adas.parquet",
        "columns": {
            "Shape": "geometry"
        },
        "tasks": {
            "convert": "todo"
        },
        "note": "rpa-parcel-adas is the closest dataset we have to that used by EVAST, and similar to WFM (noted 2024-05-14).  It was requested directly from RPA and was shared on SharePoint."
    },
    {
        "name": "rpa-hedge-adas",
        "uri": "missing sharepoint link",
        "bronze": "/dbfs/mnt/lab/restricted/ELM-Project/bronze/rpa-hedge-adas.parquet",
        "columns": {
            "Shape": "geometry"
        },
        "tasks": {
            "convert": "todo"
        },
        "note": "rpa-hedge-adas is RPA's control layer for hedgerows at the same instance as rpa-parcel-adas."
    }
]

# COMMAND ----------

add_to_catalogue(manual_datasets)
