# Databricks notebook source
# MAGIC %pip install -q xyzservices osdatahub git+https://github.com/aw-west-defra/cdap_geo.git

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F, types as T

import osdatahub
import cdap_geo
key = 'WxgUdETn6cy58WZkfwZ7wdMVLlt5eDsX'

# COMMAND ----------

pd.DataFrame(osdatahub.OpenDataDownload.all_products())

# COMMAND ----------

from cdap_geo.os import dl_packages_tidied
dl_packages_tidied(key)

# COMMAND ----------

root_dir = '/dbfs/tmp/os/raw'
id_product, id_version = '0040156306', '6307037'  # MMTOPO, Mott MacDonald
output_dir = f'{root_dir}/{id_product}-{id_version}/'
osdatahub.DataPackageDownload(key, id_product).download(id_version, output_dir)


# COMMAND ----------

root_dir = '/dbfs/tmp/os/raw'
id_product, id_version = '0040154231', '6306953'  # MMTOPO, Dan Lewis
output_dir = f'{root_dir}/{id_product}-{id_version}/'
osdatahub.DataPackageDownload(key, id_product).download(id_version, output_dir)
