# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Choropleth Plot - National Charater Areas
# MAGIC
# MAGIC **Author:** Obi Thompson Sargoni
# MAGIC
# MAGIC **Date:** 03/04/2024
# MAGIC
# MAGIC This notebook produces a choropleth plot from a processed dataset - a dataset which has values linked to parcel IDs. The chosen numeric variable of the
# MAGIC processed dataset is aggregated to National Charater Area polygons, which are used to plot the variable at national scale.
# MAGIC
# MAGIC Parcel IDs are used to link the chosen datasets with the NCA polygons. Therefore, the input dataset must contain an 'id_parcel' field.
# MAGIC
# MAGIC How to use this notebook:
# MAGIC
# MAGIC 1. Using the widgets, choose the processed dataset to plot and the plot variable.
# MAGIC 2. Enter the variable name and source into the free text widgets. These are used in the plot.
# MAGIC 3. Run the notebook
# MAGIC
# MAGIC Key processing steps:
# MAGIC 1. The plot variable is aggregated to give a single value per parcel. It is aggregated by summing values for each parcel id.
# MAGIC 2. Then the plot variable is aggregated to National Character Areas (NCAs) by calculating the mean across all parcels within each NCA
# MAGIC
# MAGIC The defaults for this notebook produce a choropleth plot of the proportion of parcels intersected by SHINE geometries
# MAGIC (non-designated historic features).

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------


import geopandas as gpd
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DoubleType, FloatType, IntegerType, LongType

from elmo_geo import register
from elmo_geo.datasets import catalogue, reference_parcels
from elmo_geo.plot.plotting import plot_choropleth_with_head_and_tail_bars

register()

# COMMAND ----------

medallions = sorted({d.medallion for d in catalogue})
default_medallion = medallions[0]
dbutils.widgets.dropdown("A - Medallion", default_medallion, medallions)

# COMMAND ----------

medallion = dbutils.widgets.get("A - Medallion")
sources = sorted({d.source for d in catalogue if d.medallion == medallion})
default_source = sources[0]
dbutils.widgets.dropdown("B - Source", default_source, sources)

# COMMAND ----------

source = dbutils.widgets.get("B - Source")
datasets = sorted({d.name for d in catalogue if (d.medallion == medallion) and (d.source == source)})
default_dataset = datasets[0]
dbutils.widgets.dropdown("C - Dataset", default_dataset, datasets)

# COMMAND ----------

dataset = dbutils.widgets.get("C - Dataset")
dataset = next(d for d in catalogue if d.name == dataset)
_ = [print(k, v, sep=":\t") for k, v in dataset.__dict__.items()]

# COMMAND ----------

# present fields of the dataset to select which to plot
fields = dataset.sdf().schema.fields
numeric_variables = [field.name for field in fields if isinstance(field.dataType, (DecimalType, DoubleType, FloatType, IntegerType, LongType))]
dbutils.widgets.dropdown("plot variable", numeric_variables[0], numeric_variables)
value_column = dbutils.widgets.get("plot variable")
print(f"\nDataset variable to plot:\t{value_column}")

formats = [".1%", ".0f", ".3f"]
dbutils.widgets.dropdown("variable format", formats[0], formats)
fmt = dbutils.widgets.get("variable format")

dbutils.widgets.text("variable name", "<enter name>")
dbutils.widgets.text("variable source", "<enter source")

variable_name = dbutils.widgets.get("variable name")
variable_source = dbutils.widgets.get("variable source")
print(f"Variable name:\t{variable_name}")
print(f"Variable source:\t{variable_source}")

# COMMAND ----------

# Use national character areas as geometry to aggregate data to
path_nca = "dbfs:/mnt/lab/unrestricted/elm/defra/national_character_areas/2021_03_29/output.parquet"
path_nca_poly = "dbfs:/mnt/lab/unrestricted/elm/defra/national_character_areas/2021_03_29/polygons.parquet"

# COMMAND ----------

dataset.sdf().display()

# COMMAND ----------

df = (dataset.sdf().groupBy("id_parcel").agg(F.mean(F.col(value_column)).alias(value_column))).toPandas()
df.head()

# COMMAND ----------

# join to complete set of parcels
df = reference_parcels.gdf().set_index("id_parcel").join(df.set_index("id_parcel"), how="left")
df

# COMMAND ----------

df_nca = spark.read.parquet(path_nca).repartition(200).toPandas()
df_nca = (
    df_nca.sort_values("proportion", ascending=False)
    .drop_duplicates(subset=["id_parcel"])
    .drop(
        columns=[
            #  "partition",
            "proportion"
        ]
    )
)
df_nca

# COMMAND ----------

# check parcel counts in each NCA
df.join(df_nca.set_index("id_parcel"), how="inner").groupby("nca_name").count()[value_column].plot.hist(figsize=(20, 6), bins=100)

# COMMAND ----------

# smallest NCAs by parcel count
df.join(df_nca.set_index("id_parcel"), how="inner").groupby("nca_name").count().sort_values(by=value_column, ascending=True).head(20)

# COMMAND ----------

# mean by NCA

# all parcels, with value set to zero where the parcel doesn't appear in the input dataset
df_all = df[[value_column]].fillna(0).join(df_nca.set_index("id_parcel"), how="inner").groupby("nca_name").mean()

# with only parcels that do appear in the input dataset
df_feature = df[[value_column]].dropna().join(df_nca.set_index("id_parcel"), how="inner").groupby("nca_name").mean()

df_feature.head(), df_all.head()

# COMMAND ----------

polygons = (
    spark.read.parquet(path_nca_poly)
    .withColumn("geometry", F.expr("ST_GeomFromWKB(geometry)"))
    .groupby("nca_name")
    .agg(F.expr("ST_Union_Aggr(geometry) as geometry"))
).toPandas()
polygons = gpd.GeoDataFrame(polygons, crs="epsg:27700").loc[:, ["nca_name", "geometry"]].set_index("nca_name")
polygons_feature = polygons.join(df_feature).reset_index().sort_values(by=value_column, ascending=False).dropna()
polygons_all = polygons.join(df_all).reset_index().sort_values(by=value_column, ascending=False).dropna()
polygons_all

# COMMAND ----------

plot_title = f"{variable_name} for land parcels in England by National Character Area - All parcels"
f = plot_choropleth_with_head_and_tail_bars(polygons_all, value_column, variable_name, variable_source, plot_title, fmt)
f.show()

# COMMAND ----------

plot_title = f"{variable_name} for land parcels in England by National Character Area - Feature parcels"
f = plot_choropleth_with_head_and_tail_bars(polygons_feature, value_column, variable_name, variable_source, plot_title, fmt)
f.show()
