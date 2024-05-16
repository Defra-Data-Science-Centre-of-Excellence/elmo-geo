# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest
# MAGIC Create vector datasets that can be easily loaded in spark, and such easily analyse large data.
# MAGIC
# MAGIC ## Format
# MAGIC (Geo)Parquet is the best option, however we must be cautious to save with encoding=WKB as Sedona's `sdf.write.format('geoparquet')` seems unstable between versions, potentially saving as a UDT rather than WKB+metadata as defined in (Geo)Parquet's specification: https://geoparquet.org/.
# MAGIC Unfortunately, with Sedona we will need the extra steps of, ST_GeomFromWKB and ST_AsBinary, for loading and writing datasets.
# MAGIC We use [BNG](https://www.ordnancesurvey.co.uk/documents/resources/guide-to-nationalgrid.pdf) ([map](https://britishnationalgrid.uk/)) instead of [GeoHash](https://geohash.softeng.co/gc), [H3](https://h3geo.org/), or [S2](https://s2geometry.io/), because most data comes in this form and 1 unit = 1 meter.
# MAGIC
# MAGIC ## Method
# MAGIC 1.  Convert any vector format to (Geo)Parquet with `ogr2ogr`.
# MAGIC 2.  Tidy the geometry by converting to CRS=EPSG:27700, reducing Precision to 1mm, and forcing 2D.
# MAGIC 3.  Partition this data, by adding an identifier (`fid`) chipping and saving according to the spatial index (`sindex`).
# MAGIC 4.  Store the dataset in silver and recording the process in catalogue.

# COMMAND ----------

from elmo_geo import register
from elmo_geo.datasets.catalogue import run_task_on_catalogue
from elmo_geo.io.convert import convert

register()

# COMMAND ----------

run_task_on_catalogue("convert", convert)
