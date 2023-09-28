# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Relict Hedges - Boundaries Method
# MAGIC
# MAGIC Enrich hedgerow data by identifying relict hedges
# MAGIC
# MAGIC Create plots to show the relict hedge classification methodology and results.

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

sf_hedgerow = "dbfs:/mnt/lab/unrestricted/elm_data/rural_payments_agency/efa_hedges/2022_06_24.parquet"

sf_boundary = 'dbfs:/mnt/lab/unrestricted/elm/elm_se/boundary_use_geometries.parquet/'

vom_trees_timestamp = "202308040848"
sf_vom_trees = f"dbfs:/mnt/lab/unrestricted/elm/elmo/hrtrees/tree_detections/tree_detections_{vom_trees_timestamp}.parquet"
tow_sp_parquet_output = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_SP_England_26062023.parquet"
tow_lidar_parquet_output = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_LiDAR_England_26062023_OBI_OLD.parquet"

#sf_parcel = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_02_07.parquet" # the parcels data I have used to get tree features
#sf_parcel = 'dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet/' # the parcels AW is using in the boundaries work

parcel_boundary_tollerance = 2

output_relict_path = "dbfs:/mnt/lab/unrestricted/elm/elm_se/unmapped_hedgerows_vom.parquet"
output_relict_excl_nfi_path = "dbfs:/mnt/lab/unrestricted/elm/elm_se/unmapped_hedgerows_vom_exclude_nfi.parquet"

nfi_parquet_path = "dbfs:/mnt/lab/unrestricted/elm_data/source_forestry_commission_open_data/dataset_national_forest_inventory_woodland_england/SNAPSHOT_2022_10_19_national_forest_inventory_woodland_england_2020/National_Forest_Inventory_Woodland_England_2020.parquet"

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

# MAGIC %md
# MAGIC
# MAGIC ## Load data and create geometries

# COMMAND ----------

parcel_boundary = lambda c, t: f"ST_SimplifyPreserveTopology(ST_MakeValid(ST_Boundary({c})), {t}) AS geometry_boundary"
genGeomFromText = lambda c, x: f"""ST_MakeValid(
                                    ST_SimplifyPreserveTopology(ST_GeomFromText({c}), {x})
                                )"""

'''
sdf_parcel = (spark.read.parquet(sf_parcel)
  .select(
    'id_parcel',
    'geometry',
    F.expr(parcel_boundary('geometry', parcel_boundary_tollerance)),
    F.expr("LEFT(id_parcel, 6) as tile"),
    F.expr("LEFT(id_parcel, 2) as major_grid")
  )
)
'''
#w = Window.orderBy("id_parcel")
sdf_boundaries = (spark.read.parquet(sf_boundary)
                   .withColumn("geometry_boundary_wkt", F.expr("ST_AsText(geometry_boundary)"))
                   .withColumn("id_boundary", F.monotonically_increasing_id() )
).cache()

sdf_hr = (spark.read.parquet(sf_hedgerow)
          .withColumn("wkb", F.col("geometry"))
          .withColumn("geometry", F.expr("ST_GeomFromWKB(wkb)"))
)

sdf_vom = (spark.read.parquet(sf_vom_trees)
           .withColumn("crown_poly_raster_geom", F.expr("ST_GeomFromText(crown_poly_raster)"))
           .withColumn("tree_id", F.monotonically_increasing_id() )
)

sdf_tow_li = spark.read.parquet(tow_lidar_parquet_output)
sdf_tow_sp = spark.read.parquet(tow_sp_parquet_output)
sdf_tow = (sdf_tow_li
              .union(
                  sdf_tow_sp.select(*list(sdf_tow_li.columns))
                  )
              #.filter(F.col('Woodland_Type_V2')=='Hedgerow')
              .withColumn("geometry_full", F.expr("ST_GeomFromText(wkt)"))
              .withColumn("geometry_generalised", F.expr(genGeomFromText('wkt', 5)))
)

nfiDF = (spark.read.parquet(nfi_parquet_path)
         .withColumn("geometry_full", F.expr("ST_GeomFromText(wkt)"))
         .withColumn("geometry_generalised", F.expr(genGeomFromText('wkt', 5)))
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Filter data to single tile

# COMMAND ----------

gdfgrid = gpd.read_file("os_bng_grids.gpkg", layer = "20km_grid")
gdfgrid5km = gpd.read_file("os_bng_grids.gpkg", layer = "5km_grid")

# COMMAND ----------

gdfgrid1km = gpd.read_file("os_bng_grids.gpkg", layer = "1km_grid")

# COMMAND ----------

tile_to_analyse = 'SS4826' #'SS9505' # "SS90NE" # no TOW daat for this tile - used for initial checking 
tile_poly = gdfgrid1km.loc[ gdfgrid1km['tile_name']==tile_to_analyse, 'geometry'].values[0]

# COMMAND ----------

#sdf_parcel = sdf_parcel.filter(F.col("tile").isin(tiles_to_analyse))

#sdf_boundaries = sdf_boundaries.filter(F.expr(f"LEFT(id_parcel, 4)='{tile_to_analyse}'"))
#sdf_hr = sdf_hr.filter(F.expr(f"LEFT(REF_PARCEL_SHEET_ID, 4)='{tile_to_analyse}'"))

'''
sdf_vom = (sdf_vom
           .filter(F.expr(f"major_grid=LEFT('{tile_to_analyse}', 2)"))
           .filter(F.expr(f"""ST_Contains(
                                 ST_GeomFromWKT('{tile_poly.wkt}'),crown_poly_raster_geom
                                )
           """))
)

#sdf_tow = sdf_tow.filter(F.expr(f"LEFT(LiDAR_Tile, 4)='{tile_to_analyse}'"))
'''

sdf_vom = (sdf_vom
           .filter(F.expr(f"major_grid=LEFT('{tile_to_analyse}', 2)"))
           .filter(F.expr(f"""ST_Intersects(
                                 ST_GeomFromWKT('{tile_poly.wkt}'),crown_poly_raster_geom
                                )"""))
)

sdf_tow = sdf_tow.filter(F.expr(f"""
                               ST_Intersects(
                                 ST_GeomFromWKT('{tile_poly.wkt}'),geometry_generalised
                                )
                            """))

sdf_boundaries = sdf_boundaries.filter(F.expr(f"ST_Intersects(ST_GeomFromText('{tile_poly.wkt}'), geometry_boundary)"))

nfiDF = (nfiDF
         .filter(F.expr(f"""
                               ST_Intersects(
                                 ST_GeomFromWKT('{tile_poly.wkt}'),geometry_generalised
                                )
                            """))
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Visualise tile data

# COMMAND ----------

df_boundaries = (sdf_boundaries
              .withColumn("geometry_boundary", F.expr("ST_AsText(geometry_boundary)"))
              .toPandas()
)

df_vom = (sdf_vom
          .drop('crown_poly_raster_geom')
          .toPandas()
)

df_tow = (sdf_tow
          .withColumn("geometry_full", F.expr("ST_AsText(geometry_full)"))
          .withColumn("geometry_generalised", F.expr("ST_AsText(geometry_generalised)"))
          .toPandas()
)

df_nfi = (nfiDF
          .withColumn("geometry_full", F.expr("ST_AsText(geometry_full)"))
          .withColumn("geometry_generalised", F.expr("ST_AsText(geometry_generalised)"))
          .toPandas()
)

# COMMAND ----------

gdf_boundaries = gpd.GeoDataFrame(df_boundaries, geometry = df_boundaries['geometry_boundary'].map(lambda x: from_wkt(x)))
gdf_vom = gpd.GeoDataFrame(df_vom, geometry=df_vom['crown_poly_raster'].map(lambda x: from_wkt(x)))
gdf_tow = gpd.GeoDataFrame(df_tow, geometry=df_tow['geometry_generalised'].map(lambda x: from_wkt(x)))
gdf_nfi = gpd.GeoDataFrame(df_nfi, geometry=df_nfi['geometry_generalised'].map(lambda x: from_wkt(x)))

# COMMAND ----------

f, ax = plt.subplots(figsize = (15,15))

gdf_boundaries.plot(ax=ax, color='grey', linestyle=':', alpha=0.5)
gdf_boundaries.loc[gdf_boundaries['elg_hedge']==True].plot(ax=ax, color='green')

gdf_vom.plot(ax=ax, facecolor = 'darkkhaki', edgecolor=None, alpha=0.5)

gdf_tow.plot(ax=ax, facecolor='fuchsia', edgecolor=None, alpha=1.0)

gdf_nfi.plot(ax=ax, facecolor='grey', edgecolor=None, alpha=1.0)

#zoom_area = (297000, 109000, 298000, 110000)
#ax.set_xlim(xmin = zoom_area[0], xmax = zoom_area[2])
#ax.set_ylim(ymin = zoom_area[1], ymax = zoom_area[3])
plt.suptitle(tile_to_analyse)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Relict hedge classification using VOM Trees and intersection proportion
# MAGIC
# MAGIC This is not the method used to produced the relict hedges dataset. Instead I used the TOW Hedges. See section below for that method.

# COMMAND ----------

def filter_out_itersecting_features(inDF, filterFeaturesDF, id_col = 'tree_id'):

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

# COMMAND ----------

# Filter out Trees that intersect woodland
sdf_vom = sdf_vom.withColumn('geometry', F.col("crown_poly_raster_geom"))
nfiDF = nfiDF.withColumn("geometry", F.col("geometry_generalised"))

sdf_vom_no_woods = filter_out_itersecting_features(sdf_vom, nfiDF, id_col = 'tree_id')

# Will also want to filter out elements from tow
'''
sdf_tow = sdf_tow.withColumn("geometry", F.col("geometry_generalised"))
sdf_tow_wd = sdf_tow.filter(F.col('Woodland_Type_V2')!='Hedgerow')

sdf_vom_no_woods_no_hr = filter_out_itersecting_features(sdf_vom_no_woods, sdf_tow_wd, id_col = 'tree_id')
'''

# COMMAND ----------

sdf_vom_input = sdf_vom_no_woods

# COMMAND ----------

# Intersect non hedgerow boundaries trees
sdf_boundaries = sdf_boundaries.withColumn("geometry", F.expr("ST_GeomFromText(geometry_boundary_wkt)"))
sdf_vom_input = sdf_vom_input.withColumn("geometry", F.col("crown_poly_raster_geom"))

sdf_boundaries_not_hedge = sdf_boundaries.filter(F.col("elg_hedge")!=True)
sdf_boundaries_tree = (sjoin(spark, sdf_boundaries_not_hedge, sdf_vom_input, lsuffix='', rsuffix='_vom')
                        .withColumn("boundary_vom_length", F.expr("ST_Length(ST_MakeValid(ST_Intersection(geometry, geometry_vom)))"))
)

# Get tree coverage of non-hedgerow sections
boundary_stats = (sdf_boundaries_tree
                 .groupby("id_parcel", "id_boundary")
                 .agg(
                     F.expr('SUM(boundary_vom_length) AS boundary_vom_length')
                 )
)

# Join segment stats back to segment dataframe
df_boundary_w_stats = (sdf_boundaries_not_hedge.join(boundary_stats, on = ['id_parcel','id_boundary'], how = 'left')
                  .withColumn("boundary_length", F.expr("ST_Length(geometry_boundary)"))
                  .fillna(0, subset = ['boundary_vom_length'])
                  .withColumn("prop_vom", F.expr("boundary_vom_length / boundary_length"))
)

# COMMAND ----------

# Filter non-relict boundaries and save
df_relict_boundaries = (df_boundary_w_stats
                        .filter(F.col("prop_vom")>0.75)
                        .select(*['id_parcel','id_boundary','geometry_boundary_wkt','boundary_vom_length','boundary_length','prop_vom'])
)
df_relict_boundaries.write.mode("overwrite").parquet(output_relict_excl_nfi_path)

# COMMAND ----------

df_rh = spark.read.parquet(output_relict_path)

rh_length = (df_rh
          .filter(F.col("prop_vom")>0.8)
          .select('boundary_length')
          .agg(F.sum("boundary_length"))
          .collect()[0][0]
)
print(rh_length)

# COMMAND ----------

# Repartition the data to align with how new methods results are partitioned
df_rh = (df_rh
         .withColumn("bng_1km", F.expr("LEFT(id_parcel, 6)"))
         .withColumn("bng_10km", F.expr("LEFT(id_parcel, 4)"))
         .withColumn("major_grid", F.expr("LEFT(id_parcel, 2)"))
)
#sdf_relict_segments.write.mode("overwrite").partitionBy("bng_10km", "bng_1km").parquet(sf_relict_segments_out)
df_rh.write.mode("overwrite").partitionBy("major_grid", "bng_10km", "bng_1km").parquet(output_relict_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Visualise results

# COMMAND ----------

df_vom_input = (sdf_vom_input
                    .drop("crown_poly_raster_geom")
                    .drop("geometry")
                    .toPandas()
)

# COMMAND ----------

# Produce geodataframes

gdf_boundaries = (sdf_boundaries
                  .drop("geometry")
                  .drop("geometry_boundary")
                  .toPandas()
                  )

gdf_boundary_w_stats = (df_boundary_w_stats
                        .drop("geometry")
                        .drop("geometry_boundary")
                        .toPandas()
)

gdf_vom = (sdf_vom
           .drop("crown_poly_raster_geom")
           .drop("geometry")
           .toPandas()
)

gdf_nfi = (nfiDF
           .drop("geometry")
           .drop('geometry_full')
           .withColumn("geometry_generalised", F.expr("ST_AsText(geometry_generalised)"))
           .toPandas()
)

#no_hr_no_wood_tree_ids = sdf_vom_no_woods_no_hr.select("tree_id").toPandas()['tree_id']

'''
gdf_boundaries_tree = (sdf_boundaries_tree
                       .select(*['id_parcel','geometry_boundary_wkt','crown_poly_raster'])
                       .toPandas()
)
'''

# COMMAND ----------

df_tow = (sdf_tow
          .drop("geometry")
          .withColumn("geometry_full", F.expr("ST_AsText(geometry_full)"))
          .withColumn("geometry_generalised", F.expr("ST_AsText(geometry_generalised)"))
          .toPandas()
)

# COMMAND ----------

gdf_boundaries = gpd.GeoDataFrame(gdf_boundaries, geometry = gdf_boundaries['geometry_boundary_wkt'].map(lambda x: from_wkt(x)))
gdf_boundary_w_stats = gpd.GeoDataFrame(gdf_boundary_w_stats, geometry = gdf_boundary_w_stats['geometry_boundary_wkt'].map(lambda x: from_wkt(x)))
#gdf_boundaries_tree = gpd.GeoDataFrame(gdf_boundaries_tree, geometry = gdf_boundaries_tree['crown_poly_raster'].map(lambda x: from_wkt(x)))
gdf_vom = gpd.GeoDataFrame(gdf_vom, geometry = gdf_vom['crown_poly_raster'].map(lambda x: from_wkt(x)))
gdf_vom_input = gpd.GeoDataFrame(df_vom_input, geometry = df_vom_input['crown_poly_raster'].map(lambda x: from_wkt(x)))
gdf_nfi = gpd.GeoDataFrame(gdf_nfi, geometry = gdf_nfi['geometry_generalised'].map(lambda x: from_wkt(x)))
gdf_tow = gpd.GeoDataFrame(df_tow, geometry = df_tow['geometry_generalised'].map(lambda x: from_wkt(x)))

# COMMAND ----------

gdf_boundary_w_stats.duplicated(subset=['id_boundary']).any()

# COMMAND ----------

gdf_tow['Woodland_Type_V2'].value_counts()

# COMMAND ----------

gdf_vom.shape[0], gdf_vom_input.shape[0]

# COMMAND ----------

# Plot again with relict hedge classification
f, axs = plt.subplots(2,2,figsize = (20,20))
axs = axs.reshape(1,-1)[0]
threshold=0.8
min_relict_hedge_length = 2

for i, ax in enumerate(axs):

    if i==0:
        ax.set_title("Input Layers")
        gdf_boundaries.plot(ax=ax, color='grey', linestyle=':', alpha=0.5)
        gdf_boundaries.loc[gdf_boundaries['elg_hedge']==True].plot(ax=ax, color='green')

        gdf_tow.loc[ gdf_tow['Woodland_Type_V2']!='Hedgerow'].plot(ax=ax, facecolor='fuchsia', edgecolor=None, alpha=1.0)
        gdf_tow.loc[ gdf_tow['Woodland_Type_V2']=='Hedgerow'].plot(ax=ax, facecolor='cyan', edgecolor=None, alpha=1.0)
        gdf_nfi.plot(ax=ax, facecolor='grey', edgecolor=None, alpha=1.0)

        gdf_vom.plot(ax=ax, facecolor = 'darkkhaki', edgecolor=None, alpha=0.8)
    
    if i==1:
        ax.set_title("Removing non-hedge trees")
        gdf_boundaries.plot(ax=ax, color='grey', linestyle=':', alpha=0.5)
        gdf_boundaries.loc[gdf_boundaries['elg_hedge']==True].plot(ax=ax, color='green')

        #gdf_tow.loc[ gdf_tow['Woodland_Type_V2']=='Hedgerow'].plot(ax=ax, facecolor='fuchsia', edgecolor=None, alpha=0.5)
        #gdf_nfi.plot(ax=ax, facecolor='grey', edgecolor=None, alpha=1.0)
        
        gdf_vom.loc[~gdf_vom['tree_id'].isin(gdf_vom_input['tree_id'])].plot(ax=ax, facecolor = 'red', edgecolor=None, alpha=0.7)
        gdf_vom_input.plot(ax=ax, facecolor = 'darkkhaki', edgecolor=None, alpha=0.7)

    if i==2:
        ax.set_title("Classifying relict hedges")
        gdf_boundaries.plot(ax=ax, color='grey', linestyle=':', alpha=0.5)
        gdf_boundaries.loc[gdf_boundaries['elg_hedge']==True].plot(ax=ax, color='green')

        #gdf_tow.loc[ gdf_tow['Woodland_Type_V2']!='Hedgerow'].plot(ax=ax, facecolor='fuchsia', edgecolor=None, alpha=0.5)
        #gdf_nfi.plot(ax=ax, facecolor='grey', edgecolor=None, alpha=1.0)

        #gdf_vom.loc[~gdf_vom['tree_id'].isin(gdf_vom_no_woods_no_hr['tree_id'])].plot(ax=ax, facecolor = 'red', edgecolor=None, alpha=0.7)
        gdf_vom_input.plot(ax=ax, facecolor = 'darkkhaki', edgecolor=None, alpha=0.7)

        # Finally add boundaries classified as relict hedge
        gdf_boundary_w_stats.loc[ gdf_boundary_w_stats['prop_vom']>threshold ].plot(ax=ax, color='red')
    
    bounds = tile_poly.bounds
    ax.set_xlim(xmin=bounds[0], xmax=bounds[2])
    ax.set_ylim(ymin=bounds[1], ymax=bounds[3])

plt.suptitle(tile_to_analyse, y=0.9)

# COMMAND ----------

# Plot again with relict hedge classification
f, axs = plt.subplots(1,2,figsize = (30,15))
axs = axs.reshape(1,-1)[0]
threshold=0.8

#gdf_bss = gdf_boundary_w_stats.loc[ (gdf_boundary_w_stats['id_parcel']=='SS97094372')]
gdf_bss = gdf_boundary_w_stats

min_relict_hedge_length = 2
for i, ax in enumerate(axs):

    # Plot all segments in grey
    gdf_bss.plot(ax = ax, color = 'grey', linestyle=':')

    gdf_boundaries.loc[gdf_boundaries['elg_hedge']==True].plot(ax=ax, color = 'green')
    gdf_vom.drop_duplicates(subset=['geometry']).plot(ax = ax, color = 'dimgrey', alpha = 0.5)
    #gdf_boundaries_tree.drop_duplicates(subset=['geometry']).plot(ax = ax, color = 'dimgrey', alpha = 0.5)

    gdf_bss.loc[(gdf_bss["prop_vom"]>p)].plot(ax=ax, color='red')
    ax.set_title(f"Threshold boundary tree proportion: {p}", fontsize = 20)

    zoom_area = (297000, 109000, 298000, 110000)
    #zoom_area = (297200, 109600, 297400, 109800)
    ax.set_xlim(xmin = zoom_area[0], xmax = zoom_area[2])
    ax.set_ylim(ymin = zoom_area[1], ymax = zoom_area[3])
    #ax.set_axis_off()
    '''
    for ix, row in gdf_bss.iterrows():
        s = str(row['id_parcel'])
        xy = list(row['geometry'].centroid.coords)[0]
        ax.annotate(s, xy=xy, color='green')
    '''

    

plt.tight_layout()

# COMMAND ----------

# Plot again with relict hedge classification

f, axs = plt.subplots(1,3,figsize = (30,15))
axs = axs.reshape(1,-1)[0]
thresholds = [1.0, 0.9, 0.8]
for i, ax in enumerate(axs):
    p = thresholds[i]

    # Plot all segments in grey
    gdf_ss.plot(ax = ax, color = 'grey')

    gdf_seg_hr.plot(ax=ax, color = 'green')
    gdf_seg_tree.plot(ax = ax, color = 'dimgrey', alpha = 0.5)

    gdf_ss.loc[gdf_ss["prop_tree"]>p].plot(ax=ax, color='red')
    ax.set_title(f"Threshold boundary tree proportion: {p}", fontsize = 20)
    #ax.set_axis_off()

plt.tight_layout()
