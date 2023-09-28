# Databricks notebook source
# MAGIC %md
# MAGIC # Woody Features
# MAGIC - Arborial Features? / Silven Features?
# MAGIC - **other woody** features are vegetation that is not hedgerow or woodland.
# MAGIC - **relict hedgerow** is a row of other woody.  *WIP
# MAGIC
# MAGIC Classifies parcel boundaries as either **hedgerow**, **woodland**, or **other woody**.
# MAGIC
# MAGIC The other woody classification is then used to classify sections of parcel boundaries as relict hedges. Relict hedges are ["where individual shrubs have grown up into single stem mature trees with full canopies and lots of gaps in between"](https://www.teagasc.ie/news--events/daily/environment/hedges-for-rejuvenation.php)
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Data
# MAGIC - Hedgerow
# MAGIC   - RPA, EFA Hedge
# MAGIC - Woodland
# MAGIC   - FR, National Forest Inventory
# MAGIC   - To do:
# MAGIC     - FR, Felling Licences
# MAGIC     - NE, Ancient Woodland Inventory
# MAGIC     - ECFP, Community Forest
# MAGIC - Other Woody Features
# MAGIC   - FR, Trees Outside of Woodland
# MAGIC   - EA + Internal, Vegetation Object Model Tree Detections
# MAGIC - Validation
# MAGIC   - To do:
# MAGIC     - NE, Living England
# MAGIC     - OS, NGD lnd-fts-land
# MAGIC     - Sentinel, NDVI
# MAGIC
# MAGIC
# MAGIC ## Methodology
# MAGIC
# MAGIC ### Parcel boundary segments
# MAGIC
# MAGIC Parcel boundaries are split into segments. This means relict hedges can be classified in terms of tree canopy density at the sub parcel boundary level.
# MAGIC
# MAGIC Parcel segments are created by:
# MAGIC   - simplifying parcel the parcel boundary geometry
# MAGIC   - dumping the boundary coordinates to a list of points
# MAGIC   - connecting consecutive points to form linestring boundary segments, each made of two points
# MAGIC
# MAGIC ### Join woody features to parcel segments
# MAGIC
# MAGIC Each woody feature layer (hedgerow, woodland, other wood) is spatially joined to parcel boundary segments.
# MAGIC
# MAGIC A buffered spatial join is used to include woody features that are close to, but not necessarily touching, parcel segments.
# MAGIC
# MAGIC ### Priority method 
# MAGIC
# MAGIC The priority method classifies portions of parcel boundary segments as _either_ hedgerow, woodland, other woody, or unclassified (meaning not a woody boundary).
# MAGIC
# MAGIC The hierarchy of this classification is:
# MAGIC   1. Hedgerow
# MAGIC   2. Woodland
# MAGIC   3. Other woody
# MAGIC   4. Unclassified
# MAGIC
# MAGIC If a portion of a boundary segment intersects hedgerow that portion is removed so that is cannot be classified as any other woody type. Only portions of boudnary segments that are not intersected y hedgerows or woodland can be classified as other woody.
# MAGIC
# MAGIC ### Relict hedge classification
# MAGIC
# MAGIC Relict hedges are classified as boundary segments with a minimum density of other woody cannopy cover.
# MAGIC
# MAGIC The basic process is:
# MAGIC   1. Calculate the length of boudnary segments intersected by other woody features
# MAGIC   2. Divide this by the total segment length to give the proportion of other woody features
# MAGIC   3. Classify the segment as a relict hedge based on a threshold proportion.
# MAGIC
# MAGIC On top of this additional filters can be applied. For example, only certain types of Trees Outside Woodland can be included in the 'other woody' classification.

# COMMAND ----------

# DBTITLE 1,Find Filepaths
from glob import glob

set(
  glob('/dbfs/mnt/base/unrestricted/source_[!bluesky]*/dataset_*fell*', recursive=True)
  + glob('/dbfs/mnt/base/unrestricted/source_[!bluesky]*/dataset_*wood*', recursive=True)
  + glob('/dbfs/mnt/base/unrestricted/source_[!bluesky]*/dataset_*forest*', recursive=True)
)

# COMMAND ----------

# DBTITLE 1,Inputs and Output Filepaths
#
# Input datasets 
#
sf_parcel = 'dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_06_03.geoparquet'
sf_efa_hedge = 'dbfs:/mnt/lab/unrestricted/elm_data/rpa/efa_control/2023_02_07.parquet'
sf_nfi = 'dbfs:/mnt/lab/unrestricted/elm_data/forestry_commission/national_forest_inventory_woodland_england_2018/2021_03_24.parquet'
sf_nfi = 'dbfs:/mnt/lab/unrestricted/elm_data/source_forestry_commission_open_data/dataset_national_forest_inventory_woodland_england/SNAPSHOT_2022_10_19_national_forest_inventory_woodland_england_2020/National_Forest_Inventory_Woodland_England_2020.parquet'
sf_fl = 'dbfs:/mnt/lab/unrestricted/elm_data/forestry_commission/felling_licences/2021_03_18.parquet'
# sf_to = 'dbfs:/mnt/lab/unrestricted/elm_data/defra/traditional_orchards/2021_03_22.parquet'
sf_cf = 'dbfs:/mnt/lab/unrestricted/elm_data/englands_community_forests_partnership/community_forests/2021_11_02.parquet'
sf_awi = 'dbfs:/mnt/lab/unrestricted/elm_data/defra/ancient_woodland_inventory/2021_03_12.parquet'
sf_vom_td = f"dbfs:/mnt/lab/unrestricted/elm/elmo/hrtrees/tree_detections/tree_detections_202308040848.parquet"
sf_tow_sp = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_SP_England_26062023.parquet"
sf_tow_lidar = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_LiDAR_England_26062023.parquet"

#
# Option to work on subset of data. Used for testing
#
filter_parcels = True
if filter_parcels:
  filter_tile = 'SP5833' #'SS4826' 'SP5833' - this tile is causing invalid geom # SP6552 - this is the tile I am creating figures for
  path_out = 'dbfs:/mnt/lab/unrestricted/elm/elm_se/testSP5833v2/elm_se-{}-2023_08.parquet' # v2 - changes to where segmenting happens (after hedge and wood priority) and joining segments back to original parcel boundararies. Output needs to be same geometry as parcel boundary.

  # partition numbers
  n1 = 100
  n2 = 100

else:
  filter_tile = None
  path_out = 'dbfs:/mnt/lab/unrestricted/elm/elm_se/testSP/elm_se-{}-2023_08.parquet'
  #path_out = 'dbfs:/mnt/lab/unrestricted/elm/elm_se/testEng/elm_se-{}-2023_08.parquet'

  # partition numbers
  n1 = 2000
  n2 = 10_000

#
# Output paths for woody features joined to parcel segments
#
sf_parcel_segments_out = path_out.format('parcel_segments')  # Parcel boundaries split into component segments
sf_hedgerows_out = path_out.format('hedgerows')  # Hedges
sf_woodland_out = path_out.format('woodland')  # Woodland
sf_nfi_out = path_out.format('nfi')  # NFI woodland only
sf_other_woody_tow_out = path_out.format('other_woody_tow')  # Trees Outside Woodland
sf_other_woody_vom_out = path_out.format('other_woody_vom_td')  # VOM Tree Detections

#
# Output paths for priority method (differencing segments with woody features)
#
sf_woody_boundaries_out = path_out.format('woody_boundaries')  # Priority Method: hedges, then woodland, then other woody features from TOW and VOM
sf_relict_segments_out = path_out.format('relict_segments')  # Length of other woody features aggregated to each boundary segment. Used to classify segments as relict by applying length proportion threshold.
sf_wood_segments_out = path_out.format('wood_segments') # Length of other woody features aggregated to each boundary segment

# COMMAND ----------

sf_parcel_segments_out

# COMMAND ----------

from elm_se import st, io
from elm_se.register import register_sedona
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()  # Spark is already created 
register_sedona(spark)

# COMMAND ----------

import geopandas as gpd
from shapely import from_wkb, from_wkt

gdfgrid1km = gpd.read_file("./obi/os_bng_grids.gpkg", layer = "1km_grid")

# COMMAND ----------

filter_polygon = gdfgrid1km.loc[ gdfgrid1km['tile_name']==filter_tile, 'geometry'].values[0]

# COMMAND ----------

filter_polygon.bounds

# COMMAND ----------

# DBTITLE 1,Create parcel segments
BUFFER = 12

#
# Load parcels
#
if filter_parcels:
  sdf_parcel = (spark.read.parquet(sf_parcel)
                .select(
                        F.expr("CONCAT(SHEET_ID, PARCEL_ID) as id_parcel"),  
                        io.load_geometry('geometry'),  # WKB>UDT (optimised)
                      )
  )
  sdf_parcel = sdf_parcel.filter(F.expr(f"ST_Intersects(geometry, ST_GeomFromText('{filter_polygon.wkt}'))"))
else:
  sdf_parcel = (spark.read.parquet(sf_parcel)
                .filter(F.col("SHEET_ID").startswith("SP")) # Select one major grid tile to test process with
                .select(
                        F.expr("CONCAT(SHEET_ID, PARCEL_ID) as id_parcel"),  
                        io.load_geometry('geometry'),  # WKB>UDT (optimised)
                      )
  )

#
# Create boundary segments
#
parcel_boundary = lambda c, s: f"ST_SimplifyPreserveTopology(ST_MakeValid(ST_Boundary({c})), {s})"
segment_query = lambda t: f"""
select mp.bng_10km, mp.bng_1km, mp.id_parcel, mp.id_segment, ST_AsText(St_LineFromMultiPoint(St_collect(mp.p1, mp.p2))) as geometry
from
(
  select d.bng_10km, d.bng_1km, d.id_parcel, d.id_segment, d.j p1, lead(d.j) over (partition by d.id_parcel order by d.id_segment) p2 from
  (
    select bng_10km, bng_1km, id_parcel, EXPLODE(ST_DumpPoints(geometry_boundary))  j, ROW_NUMBER() OVER (ORDER BY id_parcel ASC) AS id_segment from {t}
    ) d
) mp
where (p1 is not null) and (p2 is not null);
"""

(sdf_parcel
  .withColumn("bng_1km", F.expr("LEFT(id_parcel, 6)"))
  .withColumn("bng_10km", F.expr("CONCAT(LEFT(bng_1km,3), LEFT(RIGHT(bng_1km,2),1))"))
  .withColumn("geometry_boundary", F.expr(parcel_boundary("geometry", 1)))
 .createOrReplaceTempView("parcels")
)
sdf_parcel_segments = (spark.sql(segment_query("parcels"))
                       .withColumn("geometry", io.load_geometry("geometry", encoding_fn = "ST_GeomFromText"))
                       .dropDuplicates(subset = ['geometry'])
)
sdf_parcel_segments.repartition(n1, "bng_10km").write.mode("overwrite").parquet(sf_parcel_segments_out)

# COMMAND ----------

sdf_parcel_segments = spark.read.parquet(sf_parcel_segments_out)

# COMMAND ----------

# DBTITLE 1,Buffer spatial join woody features to parcel boundary segments
# Hedge
sdf_hedge = (spark.read.parquet(sf_efa_hedge)
             .select(
                "REF_PARCEL_SHEET_ID", 
                "REF_PARCEL_PARCEL_ID",
                F.expr("CONCAT(ADJACENT_PARCEL_SHEET_ID, ADJACENT_PARCEL_PARCEL_ID) as adj_parcel"),
                'LENGTH',
                io.load_geometry('wkb_geometry').alias("geometry"),
              )
)        
sdf_hedge = (st.sjoin(sdf_parcel_segments, sdf_hedge, distance=4, lsuffix='_segjoin', rsuffix='_hedge', spark=spark)
)


# Woodland
sdf_nfi = spark.read.parquet(sf_nfi).select(
  F.lit(1).alias("key"),
  F.lit('nfi').alias('source'),
  F.concat('CATEGORY', F.lit(' - '), 'IFT_IOA').alias('class'),
  io.load_geometry('wkt', encoding_fn="ST_GeomFromText").alias("geometry"),
)
sdf_fl = spark.read.parquet(sf_fl).select(
  F.lit(1).alias("key"),
  F.lit('fl').alias('source'),
  F.col('Descriptr').alias('class'),
  io.load_geometry('geometry'),
)
sdf_cf = spark.read.parquet(sf_cf).select(
  F.lit(1).alias("key"),
  F.lit('cf').alias('source'),
  F.concat('FEATDESC', F.lit(' - '), 'FEATNAME').alias('class'),
  io.load_geometry('geometry'),
)
sdf_awi = spark.read.parquet(sf_awi).select(
  F.lit(1).alias("key"),
  F.lit('awi').alias('source'),
  F.col('themname').alias('class'),
  io.load_geometry('geometry'),
)

sdf_woodland = (
  st.sjoin(sdf_parcel_segments, sdf_nfi, distance=4, lsuffix='_segjoin', rsuffix='_woodland', spark=spark)
  #.union(st.sjoin(sdf_parcel_segments, sdf_fl, distance=4, lsuffix='_segjoin', rsuffix='_woodland'))
  #.union(st.sjoin(sdf_parcel_segments, sdf_cf, distance=4, lsuffix='_segjoin', rsuffix='_woodland'))
  #.union(st.sjoin(sdf_parcel_segments, sdf_awi, distance=4, lsuffix='_segjoin', rsuffix='_woodland'))
)


# Other Woody - TOW
'''
sdf_tow_li = spark.read.parquet(sf_tow_lidar) # TOW data came as two geodatabases - lidar and sp
sdf_tow_sp = spark.read.parquet(sf_tow_sp)
sdf_tow = (sdf_tow_li # Combine two two datasets
              .union(
                  sdf_tow_sp.select(*list(sdf_tow_li.columns)) # sdf_tow_sp has 4 additional columns that we don't want
                  )
              .select(
                  'Area_length_ratio',
                  'MBG_Width',
                  'MBG_Area',
                  'MBG_Length',
                  'Average_Width',
                  'Area_Ha',
                  'Avg_ndvi',
                  'Max_ndvi',
                  'Std_ndvi',
                  'Avg_tow_prob',
                  'Max_tow_prob',
                  'Std_tow_prob',
                  'Avg_hight_m',
                  'Max_hight_m',
                  'Std_hight_m',
                  'LiDAR_Tile',       # str: 1km OS Grid tile
                  'Shape_Length',
                  'Shape_Area',
                  'geometry',         # wkb: MULTIPOLYGON
                  'Woodland_Type_V2'  # str: Woodland classification
              )
              .withColumn("geometry", io.load_geometry(column='geometry'))
)
'''

# Load dissolved TOW data
sf_tow_dissolved_sp = "dbfs:/mnt/lab/unrestricted/elm/forest_research/tow_dissolved/TOW_SP_26062023.parquet"
sdf_tow = spark.read.parquet(sf_tow_dissolved_sp)

sdf_other_woody_tow = (
  st.sjoin(sdf_parcel_segments, sdf_tow, distance=4, lsuffix='_segjoin', rsuffix='_tow', spark=spark)
)

# Other Woody - VOM-TD
sdf_vom_td = (spark.read.parquet(sf_vom_td)
              .select(
                "top_x",
                "top_y",
                "top_height",         # int; tree top height
                "chm_path",
                "msg",
                "top_point",          # wkt; POINT representing tree top
                "crown_poly_raster",  # wkt; POLYGON representing tree crown
                "major_grid"
              )
              .withColumn("geometry", io.load_geometry(column='top_point', encoding_fn='ST_GeomFromText')) # Use coordinate to join to parcels for efficiency
)
sdf_other_woody_vom = (
  st.sjoin(sdf_parcel_segments, sdf_vom_td, distance=4, lsuffix='_segjoin', rsuffix='_vom_td', spark=spark)
)

tow_class_cols = ['Woodland_Type_V2', 'Avg_hight_m', 'Shape_Area', 'Shape_Length', 'Average_Width']
vom_class_cols = ['top_height']

# COMMAND ----------

# DBTITLE 0,t
# Save the joined data
sdf_hedge.repartition(n1, "bng_10km").write.mode("overwrite").parquet(sf_hedgerows_out)
sdf_woodland.repartition(n1, "bng_10km").write.mode("overwrite").parquet(sf_nfi_out)
sdf_other_woody_tow.repartition(n2, "bng_10km").write.mode("overwrite").parquet(sf_other_woody_tow_out)
sdf_other_woody_vom.repartition(n2, "bng_10km").write.mode("overwrite").parquet(sf_other_woody_vom_out)

# COMMAND ----------

# Inputs
sdf_parcel_segments = spark.read.parquet(sf_parcel_segments_out)
sdf_hedge = spark.read.parquet(sf_hedgerows_out)
sdf_woodland = spark.read.parquet(sf_nfi_out)
sdf_other_woody_tow = spark.read.parquet(sf_other_woody_tow_out)
sdf_other_woody_vom = (spark.read.parquet(sf_other_woody_vom_out)
  .withColumn('geometry', io.load_geometry('crown_poly_raster', encoding_fn='ST_GeomFromText')) # use crown polygon when intersecting with boundary
)

# COMMAND ----------

# DBTITLE 1,Priority Method for Boundary Use - AW
# Method
def boundary_use(left, right, right_geom:str, suffix:str, buf:int=2, simp:int=0, class_cols:list=[]):
  #fn = 'ST_MakeValid({opp}({gb}, ST_MakeValid(ST_Buffer(ST_SimplifyPreserveTopology({gtmp}, {s}), {buf}))))'
  fn = """ST_MakeValid({opp}(
    ST_PrecisionReduce({gb}, 3),
    ST_PrecisionReduce(ST_MakeValid(ST_Buffer(ST_SimplifyPreserveTopology({gtmp},{s}), {buf})),3)
    ))"""
  fn_in = fn.format(opp = 'ST_Intersection', gb = 'geometry_segment', gtmp = 'geometry_tmp', s = simp, buf = buf)
  fn_out = fn.format(opp = 'ST_Difference', gb = 'geometry_segment', gtmp = 'geometry_tmp', s = simp, buf = buf)
  right = (right
    .groupby('bng_10km', 'bng_1km', 'id_parcel', 'id_segment')  # SparkDF.GroupBy takes *args (instead of list)
    .agg(F.expr(f'ST_MakeValid(ST_Union_Aggr(ST_MakeValid(ST_PrecisionReduce({right_geom},3))))  AS geometry_tmp'))
  )
  return (left.join(right, on=['bng_10km', 'bng_1km', 'id_parcel', 'id_segment'], how='left')
    .withColumn('geometry_tmp', F.expr('COALESCE(geometry_tmp, ST_GeomFromText("POINT EMPTY"))'))
    .withColumn(f'geometry_{suffix}', F.expr(fn_in))
    .withColumn('geometry_segment', F.expr(fn_out))
    .drop('geometry_tmp')
  )


#  filter tow to just select geometries that count towards relict hedge classification
sdf_other_woody_tow_relict = sdf_other_woody_tow.filter(F.expr( "Woodland_Type_V2 not in ('Small woods', 'Group of trees')"))
#sdf_other_woody_tow_relict = spark.read.parquet(sf_other_woody_tow_relict_out)

sdf_woody_boundaries = (sdf_parcel_segments
                        .filter(F.expr("ST_Length(geometry)>2"))
  .select('bng_10km', 'bng_1km', 'id_parcel', 'id_segment', F.expr('geometry AS geometry_segment'), F.expr('geometry AS geometry_segment_orig'))
  .transform(boundary_use, sdf_hedge, right_geom='geometry_hedge', suffix = 'hedgerow_boundary', simp = 0, buf = 2)
  .transform(boundary_use, sdf_woodland, right_geom='geometry_woodland', suffix='woodland_boundary', simp = 0, buf = 2)
  .transform(boundary_use, sdf_other_woody_tow_relict, right_geom = 'geometry_tow', suffix='other_woody_tow_boundary_relict', simp = 2, buf = 2, class_cols = [])
  .transform(boundary_use, sdf_other_woody_vom, right_geom = 'geometry_vom_td', suffix='other_woody_vom_boundary', simp = 2, buf = 2, class_cols = [])
)

# Output
sdf_woody_boundaries.write.parquet(sf_woody_boundaries_out, mode='overwrite')
#display(sdf_woody_boundaries)

# COMMAND ----------

# DBTITLE 1,Define Relict Hedgerow from Other Woody
# Select just segments which are intersected by some other woody geometry and calculate the length of intersection
spark.read.parquet(sf_woody_boundaries_out).createOrReplaceTempView("woody_segments")
sdf_relict_segments = spark.sql("""
                                select 
                                left(bng_10km, 2) as major_grid,
                                bng_10km,
                                bng_1km,
                                id_parcel,
                                id_segment,
                                geometry_segment_orig as geometry,
                                ST_Length(geometry_segment_orig) as segment_length,
                                0 as length_other_woody_tow,
                                /*SUM(ST_Length(geometry_other_woody_tow_boundary_relict)) as length_other_woody_tow,*/
                                SUM(ST_Length(geometry_other_woody_vom_boundary)) as length_other_woody_vom
                                from 
                                (select * from 
                                woody_segments
                                where (
                                  /*(ST_IsEmpty(geometry_other_woody_tow_boundary_relict) = false) or */
                                (ST_IsEmpty(geometry_other_woody_vom_boundary) = false)) )tbl
                                group by bng_10km, bng_1km, id_parcel, id_segment, geometry;
                                """)

# Use length and class fields to classify segments as relict
sdf_relict_segments = (sdf_relict_segments
                       .withColumn("total_other_woody_length", F.col("length_other_woody_tow")+F.col("length_other_woody_vom"))
                       .withColumn("prop_tow", F.col("length_other_woody_tow")/F.col("segment_length"))
                       .withColumn("prop_vom", F.col("length_other_woody_vom")/F.col("segment_length"))
                       .withColumn("prop_all", F.col("total_other_woody_length")/F.col("segment_length"))
)
sdf_relict_segments.write.mode("overwrite").partitionBy("major_grid", "bng_10km", "bng_1km").parquet(sf_relict_segments_out)
#sdf_relict_segments.display()

# COMMAND ----------

sdf_relict_segments = spark.read.parquet(sf_relict_segments_out)

# COMMAND ----------

# DBTITLE 1,Wooded parcel segments
spark.read.parquet(sf_woody_boundaries_out).createOrReplaceTempView("woody_segments")
sdf_wood_segments = spark.sql("""
                                select 
                                left(bng_10km, 2) as major_grid,
                                bng_10km,
                                bng_1km,
                                id_parcel,
                                id_segment,
                                geometry_segment_orig as geometry,
                                ST_Length(geometry_segment_orig) as segment_length,
                                SUM(ST_Length(geometry_woodland_boundary)) as length_woodland
                                from 
                                (select * from 
                                woody_segments
                                where (
                                (ST_IsEmpty(geometry_woodland_boundary) = false)) )tbl
                                group by bng_10km, bng_1km, id_parcel, id_segment, geometry;
                                """)

# Use length and class fields to classify segments as relict
sdf_wood_segments = (sdf_wood_segments
                       .withColumn("prop_all", F.col("length_woodland")/F.col("segment_length"))
)
sdf_wood_segments.write.mode("overwrite").partitionBy("major_grid", "bng_10km", "bng_1km").parquet(sf_wood_segments_out)
