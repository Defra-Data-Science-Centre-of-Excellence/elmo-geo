# Databricks notebook source
# MAGIC %pip install contextily rich

# COMMAND ----------

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from pyspark.sql import functions as F

# COMMAND ----------

from sedona.spark import SedonaContext
SedonaContext.create(spark)

# COMMAND ----------

hedgerows_buffer_distance = 2

timestamp = "202311231323" # 20230602 timestamp was used for original mmsg figures 

hedgerows_path = (
    "dbfs:/mnt/lab/unrestricted/elm_data/rural_payments_agency/efa_hedges/2022_06_24.parquet"
)

parcels_path = "dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet"

features_output_template = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/tree_features_{timestamp}.parquet"
)
parcel_trees_output = features_output_template.format(timestamp=timestamp)
parcel_trees_output = "dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/hrtrees_adas_parcels_elmogeo_hedges_202311231323.parquet"
#parcel_trees_output = "dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/hrtrees_adas_parcels_efa_hedge_202311231323.parquet"
hrtree_density_csv = "/dbfs/FileStore/hrtree_density_adas.csv"
save_data = True

# COMMAND ----------

treeFeaturesDF = (spark.read.parquet(parcel_trees_output)
                  .select("id_parcel", "hrtrees_count2", "hedge_length")
)

# COMMAND ----------

print(f"N parcels with hrtrees: {treeFeaturesDF.filter( F.col('hrtrees_count2')>0).count():,}")
print(f"N parcels with hedgerows: {treeFeaturesDF.filter( F.col('hedge_length')>0).count():,}")
print(f"N parcels {treeFeaturesDF.count():,}")

# COMMAND ----------

# sdf = (spark.read.parquet(parcel_trees_output)
#        .select("id_parcel", "hrtrees_count2")
#        .dropDuplicates()
# )
# hLengthDF = (hrDF
#              .withColumnRenamed("id_parcel", "idp")
#              .groupBy("idp")
#              .agg(F.sum("hedge_length").alias("hedge_length"))
# )
# sdf = (sdf
#        .join(
#            hLengthDF,
#            hLengthDF.idp == sdf.id_parcel,
#            "left")
#        .drop("idp")
# )
# sdf.display()

# COMMAND ----------

# Join hedgerow trees per parcel to hedgerow length per parcel
hrTreeDensity = (
    treeFeaturesDF
    .withColumn("hrtrees_per_m", F.col("hrtrees_count2") / F.col("hedge_length"))
    .withColumn("hrtrees_per_100m", F.col("hrtrees_per_m") * 100)
)

data = hrTreeDensity.toPandas()
data.shape

# COMMAND ----------

data.hrtrees_count2.describe()

# COMMAND ----------

data.hrtrees_per_m.describe()

# COMMAND ----------

for c in ['hrtrees_count2', 'hrtrees_per_m', 'hrtrees_per_100m']:
    data.loc[ data[c]==0, c] = np.nan
data.dropna(subset="hrtrees_per_m").hrtrees_per_m.describe()

# COMMAND ----------

threshold_density = 1.0
n_outlier = data.loc[ data["hrtrees_per_m"]>threshold_density].shape[0]
q = 1 - (n_outlier/data.dropna(subset='hrtrees_per_m').shape[0])
print(f"{q*100:.3f} percentile density above 1 HR Tree Per M")
print(f"Number of parcels above 1 tree per m: {n_outlier}")

# COMMAND ----------

# cap maximum value to threshold percentile percentile
for c in ['hrtrees_count2', 'hrtrees_per_m', 'hrtrees_per_100m']:
    data.loc[ data[c] > data[c].quantile(q), c] = data[c].quantile(q)

# COMMAND ----------

print(data.dropna(subset="hrtrees_count2").hrtrees_per_m.describe())

# COMMAND ----------

print(data.hrtrees_per_m.fillna(0).describe())

# COMMAND ----------

data.head()

# COMMAND ----------

if save_data:
    export = (data[["id_parcel", "hrtrees_count2", "hedge_length", "hrtrees_per_m", "hrtrees_per_100m"]]
              .rename(columns = {"hrtrees_count2":"hrtrees_count",
                                 })
    )
    export.to_csv(hrtree_density_csv)

# COMMAND ----------

export.head(), export.shape, export.hrtrees_count.isnull().value_counts()

# COMMAND ----------

def download_link(filepath, move=True):
    # NB filepath must be in the format dbfs:/ not /dbfs/
    # Get filename
    filename = filepath[filepath.rfind("/"):]
    # Move file to FileStore
    '''
    if move:
        dbutils.fs.mv(filepath, f"dbfs:/FileStore/{filename}")
    else:
        dbutils.fs.cp(filepath, f"dbfs:/FileStore/{filename}")
    '''
    # Construct download url
    url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/files/{filename}?o={spark.conf.get('spark.databricks.clusterUsageTags.orgId')}"
    # Return html snippet
    return f"<a href={url} target='_blank'>Download file: {filename}</a>"

download_link(hrtree_density_csv, move=False)
