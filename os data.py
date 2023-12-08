# Databricks notebook source
import os
from glob import glob
import subprocess


for f_in in glob('/dbfs/mnt/lab/unrestricted/elm_data/os/ngd/*.gpkg'):
    name = f_in.split('/')[-1].split('.')[0]
    size = os.path.getsize(f_in) / 2**30
    f_out = f'/dbfs/mnt/lab/restricted/ELM-Project/stg/os-ngd-2022.parquet/{name}'
    if True:#not os.path.exists(f_out):
        print(f'{size:.3f}GiB\t{name}\t{f_out}')
        out = subprocess.run(['./elmo_geo/io/ogr2gpq.sh', f_in, f_out], capture_output=True, text=True)
        if out.stderr:
            print(out.stderr)


# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/lab/restricted/ELM-Project/ods/

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r /dbfs/mnt/lab/restricted/ELM-Project/ods/elmo-water_sjoin-2023_11_27.parquet

# COMMAND ----------

# MAGIC %sh
# MAGIC du -sh /dbfs/mnt/lab/restricted/ELM-Project/out/parcel_geometries.parquet
