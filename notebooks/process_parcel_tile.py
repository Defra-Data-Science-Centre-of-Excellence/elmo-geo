# MAGIC %md
# MAGIC # Processing vector data and calculating intersections with land parcels
# MAGIC This notebook is used to clean up vector datasets, and to join them with the land parcels
# MAGIC dataset to get the proportion of the land parcel intersecting with each feature
# COMMAND ----------
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# COMMAND ----------

import geopandas as gpd
import pyspark.sql.functions as F
from pyspark.sql.functions import expr
from sedona.register import SedonaRegistrator

from elmo_geo.datasets import datasets
from elmo_geo.io import download_link
from elmo_geo.joins import spatial_join
from elmo_geo.preprocessing import preprocess_dataset

SedonaRegistrator.registerAll(spark)

# COMMAND ----------

name = "tiles"
path_parcels = "/mnt/lab/unrestricted/elm/elmo/parcels/polygons.parquet"
target_epsg = 27700
n_partitions = 200
simplify_tolerence: float = 0.5  # metres
max_vertices: int = 256  # per polygon (row)
required_tiles = [
    "30UUA",
    "30UUB",
    "30UVA",
    "30UVB",
    "30UVC",
    "30UVD",
    "30UVE",
    "30UVF",
    "30UVG",
    "30UWB",
    "30UWC",
    "30UWD",
    "30UWE",
    "30UWF",
    "30UWG",
    "30UXB",
    "30UXC",
    "30UXD",
    "30UXE",
    "30UXF",
    "30UXG",
    "30UYB",
    "30UYC",
    "30UYD",
    "30UYE",
    "31UCT",
    "31UDT",
    "31UDU",
]
dataset = next(d for d in datasets if d.name == name)
print(dataset)

# COMMAND ----------

# take a look at the raw data
gpd.read_file(dataset.path_read, engine="pyogrio")

# COMMAND ----------

# process the dataset
preprocess_dataset(
    spark=spark,
    path_read=dataset.path_read,
    path_write=dataset.path_polygons,
    keep_cols=dataset.keep_cols,
    rename_cols=dataset.rename_cols,
    target_epsg=target_epsg,
    n_partitions=n_partitions,
    geometry_col="geometry",
    **dataset.read_kwargs,
)

# COMMAND ----------

# take a look at the processed data
df = spark.read.parquet(dataset.path_polygons)
df.display()

# COMMAND ----------

# further process the parcels dataset to ensure validity, simplify the vertices to a tolerence,
# and subdivide large geometries
df_parcels = (
    spark.read.parquet(path_parcels)
    .withColumn("geometry", expr("ST_GeomFromWKB(hex(geometry))"))
    .withColumn("geometry", expr("ST_MakeValid(geometry)"))
    .withColumn("geometry", expr(f"ST_SimplifyPreserveTopology(geometry, {simplify_tolerence})"))
    .withColumn("geometry", expr("ST_Force_2D(geometry)"))
    .withColumn("geometry", expr("ST_MakeValid(geometry)"))
    .withColumnRenamed("id", "id_parcel")
)

# further process the feature dataset to ensure validity, simplify the vertices to a tolerence,
# and subdivide large geometries
df_feature = spark.read.parquet(dataset.path_polygons).filter(F.col("tile").isin(required_tiles))

# intersect the two datasets
(
    spatial_join(
        df_left=df_parcels,
        df_right=df_feature,
        spark=spark,
        num_partitions=10000,
    )
    .write.format("parquet")
    .save(dataset.path_output, mode="overwrite")
)

# COMMAND ----------

# show results
result = spark.read.parquet(dataset.path_output)
count = result.count()
print(f"Rows: {count:,.0f}")
# check proportion is never > 1 - if it is might mean duplicate features int he dataset
proportion_over_1 = (result.toPandas().proportion > 1.0).sum()
if proportion_over_1:
    print(
        f"{proportion_over_1:,.0f} parcels have a feature "
        f"overlapping by a proportion > 1 ({proportion_over_1/count:%})"
    )
result.display()

# COMMAND ----------

# download
pandas_df = result.toPandas()
path_feather = "/dbfs" + dataset.path_output.replace("output", dataset.name).replace(
    ".parquet", ".feather"
)
path_csv = "/dbfs" + dataset.path_output.replace("output", dataset.name).replace(".parquet", ".csv")

# convert types
for col, newtype in dataset.output_coltypes.items():
    pandas_df[col] = pandas_df[col].astype(newtype)

# output
pandas_df.to_feather(path_feather)
pandas_df.to_csv(path_csv, index=False)
displayHTML(download_link(spark, path_feather))
displayHTML(download_link(spark, path_csv))

# COMMAND ----------
# check for any issues causing prtoportion > 1
pandas_df.sort_values("proportion", ascending=False).head(20)
# COMMAND ----------
