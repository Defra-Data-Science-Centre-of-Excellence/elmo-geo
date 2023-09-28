# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Relict Hedges
# MAGIC
# MAGIC Enrich hedgerow data by identifying relict hedges
# MAGIC
# MAGIC Method:
# MAGIC * Identify sections of parcel boundaries that are predominantly intersected by trees but are not intersected by hedgerow
# MAGIC * Classify these boundary segments as relict hedges
# MAGIC
# MAGIC How to avoid classifying all sections of of parcel boundaries with trees in as relict hedges (needs to be a line of trees to be a relict hedge)? Either:
# MAGIC 1. Use TOW data and exclude 'Lone Tree' category / only use 'Hedgerow' category
# MAGIC 2. Use VOM detections and generalise boundaries and use conditions related to % intersected by perimeter trees and length of section

# COMMAND ----------

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
from shapely import from_wkt, from_wkb
import matplotlib.pyplot as plt

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
SedonaRegistrator.registerAll(spark)

# COMMAND ----------

def sjoin(
    spark,
  sdf_left:SparkDataFrame, sdf_right:SparkDataFrame,
  lsuffix:str='_left', rsuffix:str='_right'
) -> SparkDataFrame:
  # Rename
  for col in sdf_left.columns:
    if col in sdf_right.columns:
      sdf_left = sdf_left.withColumnRenamed(col, col+lsuffix)
      sdf_right = sdf_right.withColumnRenamed(col, col+rsuffix)
  geometry_left = f'left.geometry{lsuffix}'
  geometry_right = f'right.geometry{rsuffix}'
  # Add to SQL
  sdf_left.createOrReplaceTempView('left')
  sdf_right.createOrReplaceTempView('right')
  # Anti Join
  sdf = spark.sql(f'''
        SELECT left.*, right.*
        FROM left, right
        WHERE ST_Intersects({geometry_left}, {geometry_right})
        ''')
  # Remove from SQL
  spark.sql('DROP TABLE left')
  spark.sql('DROP TABLE right')
  return sdf

# COMMAND ----------

hedgerows_path = "dbfs:/mnt/lab/unrestricted/elm_data/rural_payments_agency/efa_hedges/2022_06_24.parquet"

tow_sp_parquet_output = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_SP_England_26062023.parquet"
tow_lidar_parquet_output = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_LiDAR_England_26062023.parquet"

vom_trees_path = "dbfs:/mnt/lab/unrestricted/elm/elmo/hrtrees/tree_detections/tree_detections_202308040848.parquet"

#sf_parcel = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_02_07.parquet" # the parcels data I have used to get tree features
sf_parcel = 'dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet/' # the parcels AW is using in the boundaries work

parcel_boundary_tollerance = 2

output_unmapped_hr_path = "dbfs:/mnt/lab/unrestricted/elm/elm_se/unmapped_hedgerows_tow.parquet"

# COMMAND ----------

# DBTITLE 1,Load data and create geometries
parcel_boundary = lambda c, t: f"ST_SimplifyPreserveTopology(ST_MakeValid(ST_Boundary({c})), {t}) AS geometry_boundary"
genGeomFromText = lambda c, x: f"""ST_MakeValid(
                                    ST_SimplifyPreserveTopology(ST_GeomFromText({c}), {x})
                                )"""

sdf_parcel = (spark.read.parquet(sf_parcel)
  .select(
    'id_parcel',
    'geometry',
    F.expr(parcel_boundary('geometry', parcel_boundary_tollerance)),
    F.expr("LEFT(id_parcel, 6) as tile"),
    F.expr("LEFT(id_parcel, 2) as major_grid")
  )
)

sdf_tow_hr_li = spark.read.parquet(tow_lidar_parquet_output)
sdf_tow_hr_sp = spark.read.parquet(tow_sp_parquet_output)

sdf_tow_hr = (sdf_tow_hr_li
              .union(
                  sdf_tow_hr_sp.select(*list(sdf_tow_hr_li.columns))
                  )
              .filter(F.col('Woodland_Type_V2')=='Hedgerow')
              .withColumn("geometry_full", F.expr("ST_GeomFromText(wkt)"))
              .withColumn("geometry_generalised", F.expr(genGeomFromText('wkt', 2)))
)

sdf_hr = (spark.read.parquet(hedgerows_path)
          .withColumn("wkb", F.col("geometry"))
)

sdf_vom = (spark.read.parquet(vom_trees_path)
           .withColumn("crown_poly_raster_geom", F.expr("ST_GeomFromText(crown_poly_raster)"))
)

# COMMAND ----------

# Query to split parcel boundary into simplified component segments. These segments are the input geometries used to map relict hedges.
segment_query = """select mp.major_grid, mp.tile, mp.id_parcel, mp.sid, ST_AsText(St_LineFromMultiPoint(St_collect(mp.p1, mp.p2))) as boundary_segment
from
(
  select d.major_grid, d.tile, d.id_parcel, d.sid, d.j p1, lead(d.j) over (partition by d.id_parcel order by d.sid) p2 from
  (
    select major_grid, tile, id_parcel, EXPLODE(ST_DumpPoints(geometry_boundary))  j, ROW_NUMBER() OVER (ORDER BY id_parcel ASC) AS sid from ps
    ) d
) mp
where (p1 is not null) and (p2 is not null)"""

# COMMAND ----------

#tiles_to_analyse = ["SS8045"]
#sdf_parcel = sdf_parcel.filter(F.col("tile").isin(tiles_to_analyse))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Identify relict hedges
# MAGIC
# MAGIC Find the intersection of parcel boundaries with TOW hedgerow polygons. The difference the geometries of mapped hedgerows to get unmapped (i.e. relict) hedges.

# COMMAND ----------

# Choose which tree data to use - the hedgerows frrom teh TOW dataset
tree_geom_col = "geometry_generalised"
sdf_tree = sdf_tow_hr

# COMMAND ----------

# Run operation on a major tile at a time, helps with error handling
major_tile_codes = sdf_parcel.select("major_grid").drop_duplicates().toPandas()["major_grid"].values
major_tile_codes

# COMMAND ----------

major_tile_codes = ['SS']

# COMMAND ----------

# Loop through major tiles and run relict hedge detection
for mtc in major_tile_codes:
    print(mtc)
    # Run segment reclict hedge classification based on TOW - intersection based classification
    sdf_tree = sdf_tree.withColumn("geometry", F.col(tree_geom_col))
    sdf_hr = sdf_hr.withColumn("geometry", F.expr("ST_Buffer(ST_GeomFromWKB(wkb), 2)"))

    sdf_parcel.filter(F.col("major_grid")==mtc).createOrReplaceTempView("ps")
    sdf_seg = spark.sql(segment_query)
    sdf_seg = sdf_seg.withColumn("geometry", F.expr("ST_GeomFromText(boundary_segment)"))

    # Get parcel boundary segments that intersect tow hedgerow
    df_seg_tree = (sjoin(spark, sdf_seg, sdf_tree, lsuffix='_segment', rsuffix='_tree')
                    .withColumn("geometry", F.expr("""ST_MakeValid(ST_Intersection(
                        ST_MakeValid(geometry_segment), 
                        ST_MakeValid(geometry_tree)
                        ))"""))
    )

    # overlap those segments with mapped hedgerows, and find the difference
    df_seg_hr = (sjoin(spark, df_seg_tree, sdf_hr, lsuffix='_seg_tree', rsuffix='_hr')
                .filter(F.expr("id_parcel = CONCAT(REF_PARCEL_SHEET_ID, REF_PARCEL_PARCEL_ID)"))
                .withColumn("unmapped_hr", F.expr("""ST_MakeValid(ST_Difference(
                    ST_MakeValid(
                        ST_CollectionExtract(geometry_seg_tree, 2)
                    ), 
                    ST_MakeValid(geometry_hr)))
                    """))
                .drop("major_grid", "tile", "boundary_segment")

    )

    # Join back to segment dataframe for mapping and checking
    sdf_seg = sdf_seg.drop("geometry")
    df_seg_unmapped = (sdf_seg.join(df_seg_hr, on = ['id_parcel','sid'], how = 'left')
                    .select(
                        *list(sdf_seg.columns),
                        *[F.expr(f"ST_AsText({c}) as {c}") for c in ["geometry_seg_tree", "unmapped_hr"] ]
                    )
    )
    #df_seg_tree.count(), sdf_seg.count()

    # Save results
    try:
        df_seg_unmapped.write.mode("append").partitionBy('major_grid').parquet(output_unmapped_hr_path)
    except Exception as err:
        print(err)
        continue

# COMMAND ----------

# Use to delete if needed
import shutil
#shutil.rmtree(output_unmapped_hr_path.replace("dbfs:", "/dbfs"))
