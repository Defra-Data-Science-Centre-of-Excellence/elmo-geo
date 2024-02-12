# Databricks notebook source
import os
from glob import glob

import elmo_geo
from elmo_geo.utils.settings import FOLDER_STG
from elmo_geo.io import ingest_dash

elmo_geo.register()

# COMMAND ----------

path_out = FOLDER_STG + '/os-ngd-2022.parquet/'
os.makedirs(path_out, exist_ok=True)
for f_in in glob('/dbfs/mnt/lab/unrestricted/elm_data/os/ngd/*.gpkg'):
    f_out = path_out + f_in.split('/')[-1].split('.')[0]
    ingest_dash(f_in, f_out)
