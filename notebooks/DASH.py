# Databricks notebook source
import elmo_geo
from elmo_geo.utils.settings import FOLDER_STG
from elmo_geo.io.dash import ingest_dash

elmo_geo.register()

# COMMAND ----------

'/dbfs/FileStore/Parcels4ADAS_gdb.7z', f'{FOLDER_STG}/rpa-parcels-adas.parquet'

# COMMAND ----------

# MAGIC %ls /databricks/miniconda/bin

# COMMAND ----------

# MAGIC %sh
# MAGIC PATH=$PATH:/databricks/miniconda/bin
# MAGIC export TMPDIR=/tmp
# MAGIC export PROJ_LIB=/databricks/miniconda/share/proj
# MAGIC export OGR_GEOMETRY_ACCEPT_UNCLOSED_RING=NO
# MAGIC
# MAGIC ogrinfo -so '/dbfs/FileStore/Parcels4ADAS_gdb.7z'
# MAGIC # '/dbfs/mnt/lab/restricted/ELM-Project/stg/rpa-parcels-adas.parquet'
