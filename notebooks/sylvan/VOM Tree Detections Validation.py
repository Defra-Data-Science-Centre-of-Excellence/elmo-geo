# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Validate VOM tree detections
# MAGIC
# MAGIC The VOM tree detection process uses two parameters when classifying cells in teh VOM raster data as tree tops (more parameters are involved i the process of delineating tree crowns into polygons).
# MAGIC
# MAGIC These parameters should ideally be tuned so that the tree detection process best matches ground truth data.
# MAGIC
# MAGIC This notebook provides a process for comparing the tree crown area detected by the VOM tree detection process to the Trees Outside Woodland (TOW) dataset.
# MAGIC
# MAGIC The TOW dataset is itself a model of tree locatiosn and therefore not a true source of ground truch. However, this data has undergone a more thorough validation process which includes comparisions to field samples. It therefore represents a more authoritative source of tree locations and extend than the VOM tree detections.
# MAGIC
# MAGIC ## Method
# MAGIC ### Sample of tiles to compare
# MAGIC
# MAGIC Area of England is 130,279 km². A 1% sample would cover an area of ~1,300km².
# MAGIC
# MAGIC This corresponds to ~52 5km x 5km OS GB reference tiles. 
# MAGIC
# MAGIC ### Exclude woodland from VOM detections
# MAGIC ### Calculate precision and recall by compare area of trees 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

import os
import sys
import re
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
from shapely import from_wkt, from_wkb

from elm_se import st, io, types
from elm_se.register import register_sedona
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

spark = SparkSession.builder.getOrCreate()
register_sedona(spark)

# COMMAND ----------

# write function that excludes woodland tree detections
def disjoint_filter(inDF: SparkDataFrame, filterFeaturesDF: SparkDataFrame, left_geometry: str, right_geometry: str) -> SparkDataFrame:
    
    inDF = inDF.withColumn("geometry", F.col(left_geometry))
    filterFeaturesDF = filterFeaturesDF.withColumn("geometry", F.col(right_geometry))
    
    # Create a single geometry
    filterFeaturesDF.createOrReplaceTempView("ffDF")
    filterFeaturesDF = spark.sql(
                                    '''
                                    SELECT ST_Union_Aggr(geometry) AS geometry
                                    FROM ffDF
                                    '''
    )

    assert filterFeaturesDF.count()==1

    # Filter elements that intersect this geometry
    inDF.createOrReplaceTempView("inDF")
    filterFeaturesDF.createOrReplaceTempView("ffDF")
    notIntersectDF = spark.sql(
                                '''
                                SELECT inDF.*
                                FROM inDF, ffDF
                                WHERE ST_Disjoint(inDF.geometry, ffDF.geometry)
                                '''
    )
    return notIntersectDF

def calculate_tree_crown_areas(notNfiTreesDF: SparkDataFrame, towDF: SparkDataFrame, left_geometry: str, right_geometry: str) -> tuple:
    '''
    Running on tile SS4826 takes 3.8s.

    Using spatial join function that creates a spatial index and repartitions takes 6.8s
    '''

    notNfiTreesDF = notNfiTreesDF.withColumn("geometry", F.col(left_geometry))
    towDF = towDF.withColumn("geometry", F.col(right_geometry))

    vom_crown_area = (notNfiTreesDF
                  .select(F.expr("ST_Area(geometry) as area"))
                  .agg(F.sum("area")).collect()[0][0]
    )

    tow_crown_area = (towDF
                    .select(F.expr("ST_Area(geometry) as area"))
                    .agg(F.sum("area")).collect()[0][0]
                    )
    
    compDF = st.sjoin(notNfiTreesDF, towDF, lsuffix = "_vom", rsuffix = "_tow", spark=spark)
    approximated = False
    try:
        compDF = (compDF
                .withColumn("intersection_area", F.expr("ST_Area(ST_MakeValid(ST_Intersection(geometry_vom, geometry_tow)))"))
        )
        true_positive = compDF.agg(F.sum("intersection_area")).collect()[0][0]
    except Exception as err:
        true_positive = (compDF
                         .select(F.expr("ST_Area(geometry_vom) as area"))
                         .agg(F.sum("area")).collect()[0][0]
                         )
        aproximated = True
    
    #precision = true_positive / vom_crown_area
    #recall = true_positive / tow_crown_area

    return vom_crown_area, tow_crown_area, true_positive, approximated

def filter_out_itersecting_features(inDF, filterFeaturesDF, id_col = 'tree_id'):
    intersectDF = st.sjoin(spark, inDF, filterFeaturesDF, lsuffix='_left', rsuffix='_right')
    notIntersectDF = (inDF.join(intersectDF, on = id_col, how = 'left_anti')
                     .select(
                         *list(inDF.columns)
                     )
    )
    return notIntersectDF

def validate_tree_detections_tile(sdf_vom_td: SparkDataFrame, sdf_tow: SparkDataFrame, sdf_nfi: SparkDataFrame, vom_td_canopy_geom:str, tow_canopy_geom:str) -> tuple:
    # Exclude woodland
    # - TOW methodology states that tree height polygons within 10m of NFI woodland polygons are removed
    # - replicate this by buffering nfi polygons by 10m and intersecting with tree top points
    # - TOW also imposes a minimum tree area of 20m2 threshold for lone trees and 50m2 for hedgerows. This is not currently accounted for when excluding VOM tree detections. 
    sdf_nfi = sdf_nfi.withColumn("geometry_genbuf", F.expr("ST_Buffer(geometry, 10)"))
    sdf_notNfiTrees = disjoint_filter(sdf_vom_td, sdf_nfi, left_geometry = "top_point", right_geometry = "geometry_genbuf")

    # Calculate overlap
    result = calculate_tree_crown_areas(sdf_notNfiTrees, sdf_tow, left_geometry = vom_td_canopy_geom, right_geometry = tow_canopy_geom)
    return result

def validate_tree_detections(tile_name:str, tile_wkt:str, sdf_vom_td:SparkDataFrame,  sdf_nfi:SparkDataFrame, sdf_tow:SparkDataFrame) -> pd.DataFrame:
    '''
    '''
    # Filter data to just the tile
    sdf_vom_sub = (sdf_vom
            .filter(sdf_vom.major_grid.startswith(tile_name[:2]))
            .filter(F.expr(f"""ST_Intersects(
                                    ST_GeomFromWKT('{tile_wkt}'),crown_poly_raster
                                    )"""))
    )

    sdf_tow_sub = sdf_tow.filter(F.expr(f"""
                                ST_Intersects(
                                    ST_GeomFromWKT('{tile_wkt}'),geometry_generalised
                                    )
                                """))

    sdf_nfi_sub = sdf_nfi.filter(F.expr(f"""
                                ST_Intersects(
                                    ST_GeomFromWKT('{tile_wkt}'),geometry_generalised
                                    )
                                """))

    # validate the detections for this tile
    result = validate_tree_detections_tile(sdf_vom_sub, sdf_tow_sub, sdf_nfi_sub)

    df_res = pd.DataFrame([result], columns = ['vom_crown_area', 'tow_crown_area', 'true_positive', 'approximation'])
    return  df_res

def validate_tree_detections_string_filter(tile_name:str, sdf_vom_td:SparkDataFrame,  sdf_nfi:SparkDataFrame, sdf_tow:SparkDataFrame, vom_td_canopy_geom:str="geometry", tow_canopy_geom:str="geometry") -> pd.DataFrame:
    '''
    '''
    # Filter data to just the tile
    tile_filter = F.expr(f"(major_grid='{tile_name[:2]}') and (tile_name='{tile_name}')")
    sdf_vom_tile = sdf_vom_td.filter(tile_filter)
    sdf_tow_tile = sdf_tow.filter(tile_filter)
    sdf_nfi_tile = sdf_nfi.filter(tile_filter)

    # validate the detections for this tile
    result = validate_tree_detections_tile(sdf_vom_tile, sdf_tow_tile, sdf_nfi_tile, vom_td_canopy_geom, tow_canopy_geom)
    return pd.DataFrame([result], columns = ['vom_crown_area', 'tow_crown_area', 'true_positive', 'approximation'])


def aggregated_precision_recall(df):
    p = df['true_positive'].sum() / df['vom_crown_area'].sum()
    r = df['true_positive'].sum() / df['tow_crown_area'].sum()
    f = (2 / (1/p) + (1/r))
    return p,r,f

# COMMAND ----------

# Check metadata
dfmeta = pd.read_csv("/dbfs/mnt/lab/unrestricted/elm/elmo/tree_features/tree_detections/metadata.csv")
dfmeta

# COMMAND ----------

#timestamp = "202308040848"
tree_detection_timestamp = "202308032321"

# input paths for validation
output_trees_path = f"dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/tree_detections/tree_detections_{tree_detection_timestamp}.parquet"
tow_sp_parquet_output = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_SP_England_26062023.parquet"
tow_lidar_parquet_output = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_LiDAR_England_26062023.parquet"
nfi_path = "dbfs:/mnt/lab/unrestricted/elm_data/source_forestry_commission_open_data/dataset_national_forest_inventory_woodland_england/SNAPSHOT_2022_10_19_national_forest_inventory_woodland_england_2020/National_Forest_Inventory_Woodland_England_2020.parquet"
os_gb_grid_path = "/dbfs/mnt/lab/unrestricted/elm_data/ordnance_survey/os_bng_grids.gpkg"
countries_path = "/dbfs/FileStore/Countries_December_2022_GB_BUC.gpkg"

# paths for inputs filters to sample validation areas
sf_vom_sub = f"dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/tree_detection_validation/vom_td_{tree_detection_timestamp}_validation_sample.parquet"
sf_tow_sub = f"dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/tree_detection_validation/tow__validation_sample.parquet"
sf_nfi_sub = f"dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/tree_detection_validation/nfi__validation_sample.parquet"

# where validation results area stored
validation_results_path = "/dbfs/mnt/lab/unrestricted/elm/elmo/hrtrees/tree_detection_validation/validation_results.csv"

# COMMAND ----------

# DBTITLE 1,Sample tiles to validate
seed = 1
grid_size = 5 # size of bng gridd tile to use. 5 -> 5km x 5km
area_of_england = 130_000 #km2
validation_area = area_of_england * 0.025 # 2.5% sample
n_tiles = int(np.ceil(validation_area / grid_size**2))

# load grid tiles for England
gdf_cntry = gpd.read_file(countries_path)
england_polygon = gdf_cntry.loc[gdf_cntry['CTRY22NM']=='England', 'geometry'].values[0]
gdf_tiles = gpd.read_file(os_gb_grid_path, layer = f"{grid_size}km_grid")
gdf_tiles['major_grid'] = gdf_tiles['tile_name'].map(lambda x: x[:2])
gdf_tiles = gdf_tiles.loc[ gdf_tiles.intersects(england_polygon)]
tile_proportion = n_tiles / gdf_tiles.shape[0]

# produce stratified sample of tiles - stratified by OSGB major grid
df_sampled_tiles = gdf_tiles.groupby('major_grid', group_keys=False).apply(lambda x: x.sample(frac=tile_proportion, random_state = seed)).to_wkb()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Filter datasets to sampled tiles

# COMMAND ----------

# DBTITLE 1,Filter datase to these tiles
simplify_tollerance = 2

sdf_vom = (spark.read.parquet(output_trees_path)
           .withColumn("geometry_orig", io.load_geometry("crown_poly_raster", encoding_fn='ST_GeomFromText'))
           .withColumn("geometry", F.expr(f"ST_SimplifyPreserveTopology(geometry_orig, {simplify_tollerance})"))
           .withColumn("top_point", io.load_geometry("top_point", encoding_fn="ST_GeomFromText"))
           .repartition(10_000)
)
sdf_nfi = (spark.read.parquet(nfi_path)
         .withColumn("geometry_orig", io.load_geometry("wkt", encoding_fn="ST_GeomFromText"))
         .withColumn("geometry", F.expr(f"ST_SimplifyPreserveTopology(geometry_orig, {simplify_tollerance})"))
         .repartition(1_000)
)

sdf_tow_li = spark.read.parquet(tow_lidar_parquet_output)
sdf_tow_sp = spark.read.parquet(tow_sp_parquet_output)
sdf_tow = (sdf_tow_li
              .union(
                  sdf_tow_sp.select(*list(sdf_tow_li.columns))
                  )
              .withColumn("geometry_orig", io.load_geometry("geometry"))
              .withColumn("geometry", F.expr(f"ST_SimplifyPreserveTopology(geometry_orig, {simplify_tollerance})"))
              .repartition(10_000)
)

# COMMAND ----------

# DBTITLE 1,Intersect sample tiles with data for validation
sdf_sampled_tiles = (spark.createDataFrame(df_sampled_tiles).repartition(10, 'major_grid', 'tile_name')
                     .withColumn("geometry", io.load_geometry("geometry"))
)

sdf_vom_sample = st.sjoin(sdf_vom, sdf_sampled_tiles, lsuffix='', rsuffix='_tile', spark=spark).repartition(1_000, 'major_grid', 'tile_name')
sdf_nfi_sample = st.sjoin(sdf_nfi, sdf_sampled_tiles, lsuffix='', rsuffix='_tile', spark=spark).repartition(1_000, 'major_grid', 'tile_name')
sdf_tow_sample = st.sjoin(sdf_nfi, sdf_sampled_tiles, lsuffix='', rsuffix='_tile', spark=spark).repartition(1_000, 'major_grid', 'tile_name')

# COMMAND ----------

#sdf_vom_sample.count() # 2232417

# COMMAND ----------

# DBTITLE 1,Save sampled data
sdf_vom_sample.write.mode("overwrite").parquet(sf_vom_sub)
sdf_nfi_sample.write.mode("overwrite").parquet(sf_nfi_sub)
sdf_tow_sample.write.mode("overwrite").parquet(sf_tow_sub)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Sampled data and run validation

# COMMAND ----------

# DBTITLE 1,Load sampled data
sdf_vom_sample = spark.read.parquet(sf_vom_sub)
sdf_nfi_sample = spark.read.parquet(sf_nfi_sub)
sdf_tow_sample = spark.read.parquet(sf_tow_sub)

# COMMAND ----------

# Now apply the validation function to the tiles sdf to get validation results for each tile
#sdf_validation_results = sdf_sampled_tiles.groupby("major_grid", "tile_name").apply(validate_tree_detections_string_filter, tile_name, sdf_vom_sub, sdf_nfi_sub, sdf_tow_sub)


# COMMAND ----------

# DBTITLE 1,Run validation on sampled tiles
if os.path.exists(validation_results_path):
    df_res_all = pd.read_csv(validation_results_path)
else:
    df_res_all = pd.DataFrame()

for ix, row in df_sampled_tiles.iterrows():
    print(row['tile_name'])
    df_res = validate_tree_detections_string_filter(row['tile_name'], sdf_vom_sample, sdf_nfi_sample, sdf_tow_sample)
    df_res['tile_name'] = row['tile_name']
    df_res['tile_geom'] = row['geometry']
    df_res['tree_detections_data'] = output_trees_path
    df_res['method'] = f'stratified_sample_seed_{seed}'
    df_res_all = pd.concat([df_res_all, df_res])

df_res_all['precision'] = df_res_all['true_positive'] / df_res_all['vom_crown_area']
df_res_all['recall'] = df_res_all['true_positive'] / df_res_all['tow_crown_area']
df_res_all

# COMMAND ----------

df_res_all.to_csv(validation_results_path, index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calculate pre and re across all tiles

# COMMAND ----------

df_res_all = pd.read_csv(validation_results_path)

# COMMAND ----------

s = df_res_all.loc[ df_res_all['true_positive'].isnull()==False].groupby("tree_detections_data").apply(aggregated_precision_recall)
s
