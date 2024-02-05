# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Check Parcel ID Alignment with Features
# MAGIC
# MAGIC Used to check whether parcel ids in various parcel geometries datasets match parcel ids in hedgerows and waterbodies datasets.

# COMMAND ----------

from sedona.spark import SedonaContext
from elmo_geo.utils.dbr import spark
from pyspark.sql import functions as F
SedonaContext.create(spark)

# COMMAND ----------

elmo_geo_hedgerows_path = (
    "dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-hedge-2024_01_08.parquet"
)
efa_hedges_path = "dbfs:/mnt/lab/unrestricted/elm_data/rural_payments_agency/efa_hedges/2022_06_24.parquet"
elmo_geo_waterbodies_path = "dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-water-2024_01_08.parquet"

adas_parcels_path = "dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet" # november 2021 parcels
rpa_2021_parcels_path = (
     "dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2021_03_16.parquet" # there are issues with this dataset
)

# COMMAND ----------

# DBTITLE 1,Load data
parcelsDF = (spark.read.format("geoparquet").load(adas_parcels_path)
)
parcelsRPA21DF = spark.read.parquet(rpa_2021_parcels_path)
elmoGeoHedgesDF = (spark.read.format("geoparquet").load(elmo_geo_hedgerows_path)
)
efaHedgesDF = spark.read.parquet(efa_hedges_path)
elmoGeoWaterBodyDF = spark.read.parquet(elmo_geo_waterbodies_path)

# COMMAND ----------

# DBTITLE 1,Check ADAS parcels align with elmo-geo hedgerows
compDF = (parcelsDF
          .join(elmoGeoHedgesDF, elmoGeoHedgesDF.id_parcel == parcelsDF.id_parcel, "inner")
)

print(f"N Parcels: {parcelsDF.dropDuplicates(subset=['id_parcel']).count():,}")
print(f"N hedge parcels: {elmoGeoHedgesDF.dropDuplicates(subset=['id_parcel']).count():,} ")
print(f"N hedge parcels in ADAS parcels: {compDF.dropDuplicates(subset = ['id_parcel']).count():,}")

# COMMAND ----------

# DBTITLE 1,Check ADAS parcels align with EFA Hedgerows
efaHedgesDF = (efaHedgesDF
               .withColumn("id_parcel", F.concat("REF_PARCEL_SHEET_ID", "REF_PARCEL_PARCEL_ID"))
)
compDF = (parcelsDF
          .join(efaHedgesDF, efaHedgesDF.id_parcel == parcelsDF.id_parcel, "inner")
)

print(f"N Parcels: {parcelsDF.dropDuplicates(subset=['id_parcel']).count():,}")
print(f"N EFA hedges parcels: {efaHedgesDF.dropDuplicates(subset=['REF_PARCEL_SHEET_ID', 'REF_PARCEL_PARCEL_ID']).count():,} ")
print(f"N hedge parcels in ADAS parcels: {compDF.dropDuplicates(subset = ['id_parcel']).count():,}")

# COMMAND ----------

# DBTITLE 1,Check RPA 2021 parcels align with elmo-geo hedgerows
parcelsRPA21DF = (parcelsRPA21DF
                .withColumn("id_parcel", F.concat("SHEET_ID", "PARCEL_ID"))
                )
compDF = (parcelsRPA21DF
          .join(elmoGeoHedgesDF, elmoGeoHedgesDF.id_parcel == parcelsRPA21DF.id_parcel, "inner")
)

print(f"N Parcels: {parcelsRPA21DF.dropDuplicates(subset=['id_parcel']).count():,}")
print(f"N hedge parcels: {elmoGeoHedgesDF.dropDuplicates(subset=['id_parcel']).count():,} ")
print(f"N hedge parcels in ADAS parcels: {compDF.dropDuplicates(subset = ['id_parcel']).count():,}")

# COMMAND ----------

# DBTITLE 1,Check ADAS parcels align with elmo-geo waterbodies
compDF = (parcelsDF
          .join(elmoGeoWaterBodyDF, elmoGeoWaterBodyDF.id_parcel == parcelsDF.id_parcel, "inner")
)

print(f"N Parcels: {parcelsDF.dropDuplicates(subset=['id_parcel']).count():,}")
print(f"N waterbody parcels: {elmoGeoWaterBodyDF.dropDuplicates(subset=['id_parcel']).count():,} ")
print(f"N waterbody parcels in ADAS parcels: {compDF.dropDuplicates(subset = ['id_parcel']).count():,}")
