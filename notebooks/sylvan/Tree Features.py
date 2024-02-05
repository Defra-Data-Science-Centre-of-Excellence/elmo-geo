# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Tree Features
# MAGIC
# MAGIC Objective: Produce parcel level metrics releated to the presence of trees.
# MAGIC
# MAGIC This notebook takes data on tree locations, parcel boundaries, and hedgerow locations to produce parcel tree metrics.
# MAGIC
# MAGIC ## Input Data
# MAGIC
# MAGIC Tree locations and crowns are taken from the output of the VOM tree detection notebook (), saved at
# MAGIC `dbfs:/mnt/lab/unrestricted/elm/elmo/hrtrees/tree_detections/tree_detections_{timestamp}.parquet`
# MAGIC
# MAGIC Parcel geometries are taken from
# MAGIC `dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2021_03_16.parquet`.
# MAGIC Using the March 2021 geometries to best align with the version of the WFM being used in elmo (May 2021) as of Nov 2023.
# MAGIC
# MAGIC Hedgerow geometries are already linked to parcels and located at
# MAGIC `dbfs:/mnt/lab/unrestricted/elm_data/rural_payments_agency/efa_hedges/2022_06_24.parquet`
# MAGIC
# MAGIC Hedgerow lengths are taken from
# MAGIC `dbfs:/mnt/lab/unrestricted/elm/elmo/hedgerows_and_water/hedgerows_and_water.csv`
# MAGIC
# MAGIC Water body geometries taken from
# MAGIC `dbfs:/mnt/lab/unrestricted/elm/buffer_strips/waterbody_geometries.parquet `
# MAGIC
# MAGIC ## Output Data
# MAGIC
# MAGIC > Tree Features
# MAGIC > This table has the tee features for each parcel.
# MAGIC > `dbfs:/mnt/lab/unrestricted/elm/elmo/hrtrees/tree_features_{timestamp}.parquet`
# MAGIC > The timestamp of the file path corresponds to the timestamp of the tree detection data used as an input to the notebook.
# MAGIC >
# MAGIC | SHEET_ID | PARCEL_ID | perimeter_length | SHEET_PARCEL_ID | hrtrees_count2 | wbtrees_count2  | wbtrees_count4 | perim_trees_count2 | crown_perim_length2 | int_trees_count2 | perim_trees_count4 | crown_perim_length4 | int_trees_count4
# MAGIC |---|---|---|---|---|---|---|---|---|---|---|---|---|
# MAGIC |   |   |   |   |   |   |   |   |   |   |   |   |   |
# MAGIC
# MAGIC ### Fields Metadata
# MAGIC
# MAGIC ##### SHEET_ID
# MAGIC
# MAGIC The parcel sheet ID
# MAGIC
# MAGIC ##### PARCEL_ID
# MAGIC
# MAGIC The parcel parcel ID
# MAGIC
# MAGIC ##### perimeter_length
# MAGIC
# MAGIC The length of the parcel perimeter. Calculated by getting the length of the boundary of the parcel polygon
# MAGIC
# MAGIC ##### SHEET_PARCEL_ID
# MAGIC
# MAGIC Unique identifier for each parcel. Not duplicated.
# MAGIC
# MAGIC Produced by concatenating REF_PARCEL_SHEET_ID and REF_PARCEL_PARCEL_ID
# MAGIC
# MAGIC ##### hrtrees_count{buffer}
# MAGIC
# MAGIC Number of hedgerow trees associated with this parcel.
# MAGIC
# MAGIC Given by intersecting buffered hedgerow geometries with tree crown coordinates.
# MAGIC
# MAGIC The amount hedgerows are buffered by is given in the field name. hr_tree_count2 means the hedgerow geometry was buffered 2m before intersecting with the trees.
# MAGIC
# MAGIC ##### wbtrees_count{buffer}
# MAGIC
# MAGIC Number of water body trees associated with this parcel.
# MAGIC
# MAGIC Given by intersecting buffered water body geometries with tree crown coordinates.
# MAGIC
# MAGIC Water bodies are filtered to exclude geometries tagged as ‘Sea’.
# MAGIC
# MAGIC The amount water bodies are buffered by is indicated by the number in the field name.
# MAGIC
# MAGIC ##### perim_trees_count{buffer}
# MAGIC
# MAGIC Number of trees that intersect with the parcel’s perimeter.
# MAGIC
# MAGIC The distance the parcel perimeter is buffered by is given in the field name.
# MAGIC
# MAGIC Here is a figure showing perimeter trees (in green) for a few parcels using a 2m buffer:
# MAGIC
# MAGIC ##### crown_perim_length{buffer}
# MAGIC
# MAGIC The length of the parcel perimeter that intersects with perimeter tree crowns.
# MAGIC
# MAGIC Calculated by intersecting the crown geometries (polygons) of perimeter trees (trees whose crown coordinate intersects with the buffered perimeter) with the parcel perimeter. See figure above for illustration.
# MAGIC
# MAGIC ##### int_trees_count{buffer}
# MAGIC
# MAGIC Number of trees in the parcel interior.
# MAGIC
# MAGIC The parcel interior is given by the difference between the buffered parcel perimeter and the parcel geometry. The distance the parcel perimeter is buffered by is given in the field name.
# MAGIC

# COMMAND ----------

from tree_features import *
from matplotlib import pyplot as plt
import geopandas as gpd
from shapely import from_wkb, from_wkt
from sedona.spark import SedonaContext
#from sedona.register import SedonaRegistrator
from elmo_geo.utils.dbr import spark

# COMMAND ----------

SedonaContext.create(spark)
#SedonaRegistrator.registerAll(spark)

# COMMAND ----------

hedgerowBufferDistances = [2]
parcelBufferDistances = [2, 4]
waterbodyBufferDistances = [2, 4]
parcel_buffer_distance = 4

timestamp = "202311231323"  # 202311231323 timestamp is tree detection version with best F1 score so far.

elmo_geo_hedgerows_path = (
    "dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-hedge-2024_01_08.parquet"
)
elmo_geo_waterbodies_path = "dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-water-2024_01_08.parquet"

adas_parcels_path = "dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet" # november 2021 parcels

trees_output_template = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/"
    "tree_features/tree_detections/"
    "tree_detections_{timestamp}.parquet"
)
output_trees_path = trees_output_template.format(timestamp=timestamp)

features_output_template = (
    "dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/tree_features_{timestamp}.parquet"
)
parcel_trees_output = features_output_template.format(timestamp=timestamp)

# COMMAND ----------

parcel_trees_output

# COMMAND ----------

output_trees_path

# COMMAND ----------

# Just SP and SU major tiles
# timestamp = "202306071149"
# output_trees_path = f"dbfs:/mnt/lab/unrestricted/elm/elmo/hrtrees/tree_detections/SP_SU_tree_detections_{timestamp}.parquet"
# parcel_trees_output = f"dbfs:/mnt/lab/unrestricted/elm/elmo/hrtrees/SP_tree_features_{timestamp}.parquet"

# COMMAND ----------

# DBTITLE 1,Load Data
treesDF = (spark.read.parquet(output_trees_path)
           .repartition(200_000, "major_grid", "chm_path")
)
parcelsDF = (spark.read.format("geoparquet").load(adas_parcels_path)
             .repartition(1_250, "sindex")
)
hrDF = (spark.read.format("geoparquet").load(elmo_geo_hedgerows_path)
        .repartition(1_250, "sindex")
)
wbDF = (spark.read.parquet(elmo_geo_waterbodies_path)
        .repartition(1_250, "sindex")
)

# COMMAND ----------

for DF in [parcelsDF, hrDF, wbDF, treesDF]:
    c = DF.count()
    p = DF.rdd.getNumPartitions()
    print(f"Rows per partition: {c/p}")

# COMMAND ----------

hrDF.display()

# COMMAND ----------

treesDF.select(F.col("chm_path")).display()

# COMMAND ----------

# DBTITLE 1,Prepare elmo geo hedge data for hrtree classification
# elmo-hedges assigns any hedge within 12m of a parcel to that parcel
pDF = (parcelsDF
       .withColumnRenamed("geometry", "geometry_parcel")
       .withColumnRenamed("id_parcel", "id_parcel_main")
       .select("id_parcel_main", "geometry_parcel")
)

hrDF = (hrDF
        .join(pDF, pDF.id_parcel_main == hrDF.id_parcel, "inner")
        .withColumn("geometry", F.expr(f"""
                                             ST_Intersection(
                                                 geometry,
                                                 ST_Buffer(geometry_parcel, {parcel_buffer_distance})
                                                 )"""))
        .withColumn("hedge_length", F.expr("ST_Length(geometry)"))
        .select("id_parcel", "geometry", "hedge_length", "sindex")
)

wbDF = (wbDF
        .join(pDF, pDF.id_parcel_main == wbDF.id_parcel, "inner")
        .withColumn("geometry", F.expr(f"""
                                             ST_Intersection(
                                                 geometry,
                                                 ST_Buffer(geometry_parcel, {parcel_buffer_distance})
                                                 )"""))
        .select("id_parcel", "geometry", "sindex")
)

# COMMAND ----------

#wbDF.display()

# COMMAND ----------

# DBTITLE 1,Map hedge data to check it
geo_cols = ["geometry"]
gdf = (hrDF
       .select(
           [
               "id_parcel",
               "sindex",
           *[F.expr(f"ST_AsBinary({c}) as {c}") for c in geo_cols]
           ]
       )
       .filter(F.col("sindex") == "SP53")).toPandas()
for c in geo_cols:
    gdf[c] = gdf[c].map(lambda x: from_wkb(x))

gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs = "epsg:27700")
m = gdf.explore()
outfp = "/dbfs/FileStore/hedge_map.html"
m.save(outfp)

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

download_link(outfp, move=False)

# COMMAND ----------

# Filter to a single tile, for testing
# tile_to_visualise = "SP65nw"
# tile_to_visualise = "SP"
# treesDF = treesDF.filter(f"chm_path like '%{tile_to_visualise}%'")
# parcelsDF = parcelsDF.filter(f"SHEET_ID like '{tile_to_visualise[:2]}%'")
# hrDF = hrDF.filter(f"REF_PARCEL_SHEET_ID like '{tile_to_visualise[:2]}%'")
# wbDF = wbDF.filter(f"id_parcel like '{tile_to_visualise[:2]}%'")

# COMMAND ----------

# DBTITLE 1,Get tree features
# was taking over 12hrs so I cancelled - need to find a better way.
pTreesDF = get_parcel_tree_features(
    spark,
    treesDF,
    parcelsDF,
    hrDF,
    None, # wbDF not working currently.
    parcelBufferDistances,
    hedgerowBufferDistances,
    waterbodyBufferDistances,
    double_count=True,
    report_counts=False,
)

# COMMAND ----------

pTreesDF.write.mode("overwrite").parquet(parcel_trees_output)

# COMMAND ----------

# DBTITLE 1,Add hedgerow length
pTreesDF = spark.read.parquet(parcel_trees_output)
hLengthDF = (hrDF
             .withColumnRenamed("id_parcel", "idp")
             .groupBy("idp")
             .agg(F.sum("hedge_length").alias("hedge_length"))
)
pTreesDF = (pTreesDF
       .join(
           hLengthDF,
           hLengthDF.idp == pTreesDF.id_parcel,
           "left")
       .drop("idp")
)

# COMMAND ----------

parcel_trees_output

# COMMAND ----------

# DBTITLE 1,Save the data
# Save the data
pTreesDF.write.mode("overwrite").parquet(parcel_trees_output)

# COMMAND ----------

pTreesDF.display()
