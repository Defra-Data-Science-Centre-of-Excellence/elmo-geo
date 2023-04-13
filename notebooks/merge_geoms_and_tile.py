# Databricks notebook source
# MAGIC %md
# MAGIC # Merging df with parcels geometries
# MAGIC This notebook is used to merge land parcels data frame with parcel and tile dataframe

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pyspark.sql.functions import concat, expr
from sedona.register import SedonaRegistrator

SedonaRegistrator.registerAll(spark)

# COMMAND ----------

path_tile = "dbfs:/mnt/lab/unrestricted/elm/sentinel/tiles/2023_01_01/output.parquet"
path_geoms = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_02_07.parquet"
path_output = "dbfs:/mnt/lab/unrestricted/elm/sentinel/tiles/2023_01_01/parcels.parquet"
simplify_tolerence: float = 0.5  # metres
max_vertices: int = 256  # per polygon (row)

# COMMAND ----------

# geometry dataframe
df_geoms = (
    spark.read.parquet(path_geoms)
    .withColumn("id_parcel", concat("SHEET_ID", "PARCEL_ID"))
    .withColumn("geometry", expr("ST_GeomFromWKB(wkb_geometry)"))
    .withColumn("geometry", expr("ST_MakeValid(geometry)"))
    .withColumn("geometry", expr(f"ST_SimplifyPreserveTopology(geometry, {simplify_tolerence})"))
    .withColumn("geometry", expr("ST_Force_2D(geometry)"))
    .withColumn("geometry", expr("ST_MakeValid(geometry)"))
    .select("id_parcel", "geometry")
)
display(df_geoms)

# COMMAND ----------

df_parcel = spark.read.parquet(path_tile)
display(df_parcel)

# COMMAND ----------

df_parcel = df_parcel.filter(df_parcel.proportion > 0.99).select("tile", "id_parcel")
display(df_parcel)

# COMMAND ----------

# join parcel and geometry dataframe - inner requires less computing power
#  and all parcels in tiles dataframe should be in geoms dataframe
df = (
    df_parcel.join(df_geoms, ["id_parcel"], "inner")
    .select("id_parcel", "tile", "geometry")
    .withColumn("geometry", expr("ST_AsBinary(geometry)"))
)
df.write.format("parquet").save(path_output, mode="overwrite")

# COMMAND ----------

display(df)

# COMMAND ----------

rows_geoms = df_geoms.count()
rows_tiles = df_parcel.count()
rows_join = df.count()

print(
    f"DataFrame geometry rows count : {rows_geoms:0,f}\n"
    f"DataFrame tile rows count : {rows_tiles:0,f}\n"
    f"DataFrame joined rows count : {rows_join:0,f}\n"
)

# COMMAND ----------

df_id = df.select("id_parcel")
print(f"There are{df_id.distinct().count():0,f} unique parcels.")

# COMMAND ----------
