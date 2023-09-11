# Databricks notebook source
# MAGIC %md
# MAGIC # Intersecting tile data with land parcels
# MAGIC This notebook is used to join sentinel tiles dataset with the land parcels
# MAGIC dataset to get the proportion of the land parcel intersecting with each tile.
# MAGIC It will then input the geometries back into the dataset so it can be ready for use
# MAGICV in the calc_bare_soil_perc notebook.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %pip install beautifulsoup4 lxml

# COMMAND ----------

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import pyspark.sql.functions as F
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, expr
from sedona.register import SedonaRegistrator

from elmo_geo.datasets import tiles
from elmo_geo.log import LOG
from elmo_geo.preprocessing import geometry_to_wkb, make_geometry_valid, transform_crs
from elmo_geo.sentinel import sentinel_tiles

spark = SparkSession.getActiveSession()
SedonaRegistrator.registerAll(spark)

# COMMAND ----------

names = sorted([f"{v.name}" for v in tiles.versions])
dbutils.widgets.dropdown("version", names[-1], names)
version = dbutils.widgets.get("version")

[print(k, v, sep=":\t") for k, v in tiles.__dict__.items()]
path_parcels = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_02_07.parquet"
path_output = f"dbfs:/mnt/lab/unrestricted/elm/sentinel/tiles/{version}/parcels.parquet"
target_epsg = 27700
n_partitions = 200
simplify_tolerence: float = 0.5  # metres
max_vertices: int = 256  # per polygon (row)
path_read = next(v.path_read for v in tiles.versions if v.name == version)
required_tiles = sentinel_tiles.copy()

# COMMAND ----------

# take a look at the raw data
pdf = pd.read_parquet(path_read)
gdf = gpd.GeoDataFrame(
    pdf, geometry=gpd.GeoSeries.from_wkb(pdf["geometry"], crs=target_epsg), crs=target_epsg
)
gdf

# COMMAND ----------

# process the dataset
df = (
    gdf.explode(index_parts=False)
    .pipe(transform_crs, target_epsg=27700)
    .filter(tiles.keep_cols, axis="columns")
    .rename(columns=tiles.rename_cols)
    .pipe(make_geometry_valid)
    .pipe(geometry_to_wkb)
)

LOG.info(f"Dataset has {df.size:,.0f} rows")
LOG.info(f"Dataset has the following columns: {df.columns.tolist()}")
(
    spark.createDataFrame(df)
    .repartition(n_partitions)
    .write.format("parquet")
    .save(tiles.path_polygons.format(version=version), mode="overwrite")
)
LOG.info(f"Saved preprocessed dataset to {tiles.path_polygons.format(version=version)}")

# COMMAND ----------

# take a look at the processed data
df = spark.read.parquet(tiles.path_polygons.format(version=version))
df.display()

# COMMAND ----------

# process the parcels dataset to ensure validity, simplify the vertices to a tolerence
df_parcels = (
    spark.read.parquet(path_parcels)
    .withColumn("id_parcel", concat("SHEET_ID", "PARCEL_ID"))
    .withColumn("geometry", expr("ST_GeomFromWKB(wkb_geometry)"))
    .withColumn("geometry", expr("ST_MakeValid(geometry)"))
    .withColumn("geometry", expr(f"ST_SimplifyPreserveTopology(geometry, {simplify_tolerence})"))
    .withColumn("geometry", expr("ST_Force_2D(geometry)"))
    .withColumn("geometry", expr("ST_MakeValid(geometry)"))
    .select("id_parcel", "geometry")
    .repartition(10000)
)
df_parcels.display()

# COMMAND ----------

# process the feature dataset to ensure validity, simplify the vertices to a tolerence,
# and subdivide large geometries
df_feature = (
    spark.read.parquet(tiles.path_polygons.format(version=version))
    .withColumn("geometry", expr("ST_GeomFromWKB(hex(geometry))"))
    .withColumn("geometry", expr("ST_MakeValid(geometry)"))
    .withColumn("geometry", expr(f"ST_SimplifyPreserveTopology(geometry, {simplify_tolerence})"))
    .withColumn("geometry", expr("ST_Force_2D(geometry)"))
    .withColumn("geometry", expr("ST_MakeValid(geometry)"))
    .filter(F.col("tile").isin(required_tiles))
    .repartition(1)
)
df_feature.display()

# COMMAND ----------

# Intersection using SQL as there are a small number of tiles
df_parcels.createOrReplaceTempView("parcels")
df_feature.createOrReplaceTempView("feature")

(
    spark.sql(
        """
        SELECT parcels.id_parcel, feature.tile, parcels.geometry, feature.geometry AS geom_feature
        FROM parcels, feature
        WHERE ST_INTERSECTS(parcels.geometry, feature.geometry)
        """
    )
    .repartition("tile")
    .withColumn("area_parcel", expr("ST_Area(geometry)"))
    .withColumn("int_geometry", expr("ST_INTERSECTION(geometry, geom_feature)"))
    .withColumn("area_intersection", expr("ST_Area(int_geometry)"))
    .withColumn("proportion", col("area_intersection") / col("area_parcel"))
    .filter("proportion > 0.90")
    .select("id_parcel", "tile", "geometry", "proportion")
    .withColumn("geometry", expr("ST_AsBinary(geometry)"))
    .write.format("parquet")
    .save(tiles.path_output.format(version=version), mode="overwrite")
)

# COMMAND ----------

result = spark.read.parquet(tiles.path_output.format(version=version))
count = result.count()
LOG.info(f"Rows: {count:,.0f}")
result.display()

# COMMAND ----------

pandas_df = result.toPandas()
# check for any issues causing prtoportion > 1
pandas_df.sort_values("proportion", ascending=False).head(5)

# COMMAND ----------

# count number of proprtions less than 1
pandas_df["bins"] = pd.cut(
    pandas_df["proportion"], [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
)
proportion_df = pandas_df.groupby(by="bins").count()
proportion_df

# COMMAND ----------

# visualising counting how many parcels are in a number of tiles
pdf = df_parcels.toPandas()
resultpd = result.toPandas()

counts = resultpd.groupby("id_parcel").proportion.count().value_counts()
before = set(pdf.id_parcel.values)
after = set(resultpd.id_parcel.values)
missing = before.difference(after)
counts[0] = len(missing)
counts = counts.sort_index()

sns.set_theme(context="notebook", style="white", palette="husl")
fig, ax = plt.subplots(figsize=(6, 4), constrained_layout=True)
colours = sns.color_palette("husl", n_colors=len(counts)).as_hex()
bc = ax.barh(counts.index, counts, color=colours, alpha=0.8)
ax.invert_yaxis()
sns.despine(left=True, right=True, top=True, bottom=True)
ax.bar_label(bc, [f"{c:,.0f}" for c in counts])
ax.set_xticklabels([])
ax.set_title("Parcel count by number of tile intersections", loc="left", fontsize="large")
fig.show()

# COMMAND ----------
