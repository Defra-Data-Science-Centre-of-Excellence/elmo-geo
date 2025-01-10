# Databricks notebook source
# MAGIC %md
# MAGIC # Arable Land Classification
# MAGIC Group ALC areas by grade.  Method 1 groups ALC geometries.  Method 2 group WFM parcel geometries.
# MAGIC
# MAGIC | alc_grade        | ha (alc)   | % (alc) | ha (parcels) | % (parcel) |
# MAGIC | :--------------- | ---------: | ------: | -----------: | ---------: |
# MAGIC | Exclusion        |      1,646 |    0.0% |          224 |       0.0% |
# MAGIC | Grade 1          |    354,585 |    2.7% |      295,212 |       3.0% |
# MAGIC | Grade 2          |  1,849,074 |   14.2% |    1,563,855 |      16.0% |
# MAGIC | Grade 3          |  6,290,210 |   48.2% |    5,050,146 |      51.8% |
# MAGIC | Grade 4          |  1,840,050 |   14.1% |    1,454,498 |      14.9% |
# MAGIC | Grade 5          |  1,100,734 |    8.4% |    1,034,651 |      10.6% |
# MAGIC | Non Agricultural |    656,189 |    5.0% |      306,345 |       3.1% |
# MAGIC | Urban            |    951,513 |    7.3% |       52,753 |       0.5% |
# MAGIC | Total            | 13,044,001 |  100.0% |    9,757,684 |     100.0% |

# COMMAND ----------

from elmo_geo import register
from elmo_geo.datasets import alc_parcels, alc_raw, reference_parcels
from pyspark.sql import functions as F
register()


(
    alc_raw.sdf()
    .groupby("alc_grade")
    .agg(F.expr("ROUND(SUM(st_area_sh)/10000)").alias("ha"))
).display()

(
    alc_parcels.sdf()
    .join(reference_parcels.sdf().select("id_parcel", "area_ha"), on="id_parcel")
    .groupby("alc_grade")
    .agg(F.expr("ROUND(SUM(area_ha * proportion))").alias("ha"))
).display()
