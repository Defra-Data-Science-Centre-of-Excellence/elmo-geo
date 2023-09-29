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
    glob("/dbfs/mnt/base/unrestricted/source_[!bluesky]*/dataset_*fell*", recursive=True)
    + glob("/dbfs/mnt/base/unrestricted/source_[!bluesky]*/dataset_*wood*", recursive=True)
    + glob("/dbfs/mnt/base/unrestricted/source_[!bluesky]*/dataset_*forest*", recursive=True)
)

# COMMAND ----------

# DBTITLE 1,Inputs and Output Filepaths
BUFFER = 4

#
# Input datasets
#
sf_parcel = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_06_03.geoparquet"
sf_efa_hedge = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/efa_control/2023_02_07.parquet"
sf_nfi = "dbfs:/mnt/lab/unrestricted/elm_data/forestry_commission/national_forest_inventory_woodland_england_2018/2021_03_24.parquet"
sf_nfi = "dbfs:/mnt/lab/unrestricted/elm_data/source_forestry_commission_open_data/dataset_national_forest_inventory_woodland_england/SNAPSHOT_2022_10_19_national_forest_inventory_woodland_england_2020/National_Forest_Inventory_Woodland_England_2020.parquet"
sf_fl = (
    "dbfs:/mnt/lab/unrestricted/elm_data/forestry_commission/felling_licences/2021_03_18.parquet"
)
# sf_to = 'dbfs:/mnt/lab/unrestricted/elm_data/defra/traditional_orchards/2021_03_22.parquet'
sf_cf = "dbfs:/mnt/lab/unrestricted/elm_data/englands_community_forests_partnership/community_forests/2021_11_02.parquet"
sf_awi = "dbfs:/mnt/lab/unrestricted/elm_data/defra/ancient_woodland_inventory/2021_03_12.parquet"
sf_vom_td = "dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/tree_detections/tree_detections_202308040848.parquet"
sf_tow_sp = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_SP_England_26062023.parquet"
sf_tow_lidar = (
    "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_LiDAR_England_26062023.parquet"
)

#
# Option to work on subset of data. Used for testing
#
filter_parcels = True
if filter_parcels:
    filter_tile = "SP6552"  # 'SS4826' 'SP5833' - this tile is causing invalid geom # SP6552 - this is the tile I am creating figures for
    path_out = (
        "dbfs:/mnt/lab/unrestricted/elm/elm_se/relict_hedge/test"
        + filter_tile
        + "/elm_se-{}-2023_08.parquet"
    )  # v2 - changes to where segmenting happens (after hedge and wood priority) and joining segments back to original parcel boundararies. Output needs to be same geometry as parcel boundary.

    # partition numbers
    n1 = 10
    n2 = 10

else:
    filter_tile = None
    path_out = "dbfs:/mnt/lab/unrestricted/elm/elm_se/relict_hedge/elm_se-{}-2023_08.parquet"

    # partition numbers
    n1 = 2000
    n2 = 10_000

#
# Output paths for woody features joined to parcel segments
#
sf_parcels_out = path_out.format("parcels")  # Parcel boundaries split into component segments
sf_parcel_segments_out = path_out.format(
    "parcel_segments"
)  # Parcel boundaries split into component segments
sf_hedgerows_out = path_out.format("hedgerows")  # Hedges
sf_woodland_out = path_out.format("woodland")  # Woodland
sf_nfi_out = path_out.format("nfi")  # NFI woodland only
sf_available_segments_out = path_out.format("available_segments")
sf_other_woody_tow_out = path_out.format("other_woody_tow")  # Trees Outside Woodland
sf_other_woody_vom_out = path_out.format("other_woody_vom_td")  # VOM Tree Detections
sf_other_woody_segments_out = path_out.format(
    "other_woody_segments"
)  # Other woody features priority on available segments

#
# Output paths for priority method (differencing segments with woody features)
#
sf_woody_boundaries_out = path_out.format(
    "woody_boundaries"
)  # Priority Method: hedges, then woodland, then other woody features from TOW and VOM
sf_relict_segments_out = path_out.format(
    "relict_segments"
)  # Length of other woody features aggregated to each boundary segment. Used to classify segments as relict by applying length proportion threshold.

# COMMAND ----------

sf_relict_segments_out

# COMMAND ----------

import geopandas as gpd
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T
from shapely.geometry import LineString

from elmo_geo import register
from elmo_geo.io import io2 as io
from elmo_geo.st import st
from elmo_geo.utils.dbr import spark

register()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Load parcels

# COMMAND ----------

# DBTITLE 0,Create parcel segments
#
# Load parcels
#
if filter_parcels:
    gdfgrid1km = gpd.read_file("./obi/os_bng_grids.gpkg", layer="1km_grid")
    filter_polygon = gdfgrid1km.loc[gdfgrid1km["tile_name"] == filter_tile, "geometry"].values[0]

    sdf_parcel = spark.read.parquet(sf_parcel).select(
        F.expr("CONCAT(SHEET_ID, PARCEL_ID) as id_parcel"),
        io.load_geometry("geometry"),  # WKB>UDT (optimised)
    )
    sdf_parcel = sdf_parcel.filter(
        F.expr(f"ST_Intersects(geometry, ST_GeomFromText('{filter_polygon.wkt}'))")
    )
else:
    sdf_parcel = (
        spark.read.parquet(sf_parcel)
        # .filter(F.col("SHEET_ID").startswith("SP")) # Select one major grid tile to test process with
        .select(
            F.expr("CONCAT(SHEET_ID, PARCEL_ID) as id_parcel"),
            io.load_geometry("geometry"),  # WKB>UDT (optimised)
        )
    )

sdf_parcel = (
    sdf_parcel.withColumn("major_grid", F.expr("LEFT(id_parcel,2)"))
    .withColumn("bng_1km", F.expr("LEFT(id_parcel, 6)"))
    .withColumn("bng_10km", F.expr("CONCAT(LEFT(bng_1km,3), LEFT(RIGHT(bng_1km,2),1))"))
    .withColumn(
        "geometry_boundary",
        F.expr("ST_MakeValid(ST_Boundary(geometry)) AS geometry_boundary"),
    )
)
sdf_parcel.repartition(n1, "major_grid", "bng_10km").write.mode("overwrite").parquet(sf_parcels_out)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Join hedges and woodland to parcels

# COMMAND ----------

sdf_parcels = spark.read.parquet(sf_parcels_out)

# COMMAND ----------

# DBTITLE 1,Buffer spatial join woody features to parcel boundary segments
# Hedge
sdf_hedge = spark.read.parquet(sf_efa_hedge).select(
    "REF_PARCEL_SHEET_ID",
    "REF_PARCEL_PARCEL_ID",
    F.expr("CONCAT(ADJACENT_PARCEL_SHEET_ID, ADJACENT_PARCEL_PARCEL_ID) as adj_parcel"),
    "LENGTH",
    io.load_geometry("wkb_geometry").alias("geometry"),
)
sdf_hedge = st.sjoin(
    sdf_parcels, sdf_hedge, distance=BUFFER, lsuffix="_segjoin", rsuffix="_hedge", spark=spark
)


# Woodland
sdf_nfi = spark.read.parquet(sf_nfi).select(
    F.lit(1).alias("key"),
    F.lit("nfi").alias("source"),
    F.concat("CATEGORY", F.lit(" - "), "IFT_IOA").alias("class"),
    io.load_geometry("wkt", encoding_fn="ST_GeomFromText").alias("geometry"),
)
sdf_fl = spark.read.parquet(sf_fl).select(
    F.lit(1).alias("key"),
    F.lit("fl").alias("source"),
    F.col("Descriptr").alias("class"),
    io.load_geometry("geometry"),
)
sdf_cf = spark.read.parquet(sf_cf).select(
    F.lit(1).alias("key"),
    F.lit("cf").alias("source"),
    F.concat("FEATDESC", F.lit(" - "), "FEATNAME").alias("class"),
    io.load_geometry("geometry"),
)
sdf_awi = spark.read.parquet(sf_awi).select(
    F.lit(1).alias("key"),
    F.lit("awi").alias("source"),
    F.col("themname").alias("class"),
    io.load_geometry("geometry"),
)

"""
sdf_woodland = (sdf_nfi
                .union(sdf_fl)
                .union(sdf_cf)
                .union(sdf_awi)
                .groupby("key")
                .agg(F.expr(f'ST_Union_Aggr(geometry)  AS geometry'))
)
sdf_woodland = st.sjoin(sdf_parcel_segments, sdf_woodland, distance=4, lsuffix='_segjoin', rsuffix='_woodland', spark=spark)
"""

sdf_woodland = (
    st.sjoin(
        sdf_parcels, sdf_nfi, distance=BUFFER, lsuffix="_segjoin", rsuffix="_woodland", spark=spark
    )
    # .union(st.sjoin(sdf_parcel_segments, sdf_fl, distance=4, lsuffix='_segjoin', rsuffix='_woodland'))
    # .union(st.sjoin(sdf_parcel_segments, sdf_cf, distance=4, lsuffix='_segjoin', rsuffix='_woodland'))
    # .union(st.sjoin(sdf_parcel_segments, sdf_awi, distance=4, lsuffix='_segjoin', rsuffix='_woodland'))
)

# COMMAND ----------

# DBTITLE 0,t
# Save the joined data
sdf_hedge.repartition(n1, "major_grid", "bng_10km").write.mode("overwrite").parquet(
    sf_hedgerows_out
)
sdf_woodland.repartition(n1, "major_grid", "bng_10km").write.mode("overwrite").parquet(sf_nfi_out)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Priority method - hedges and woodland

# COMMAND ----------

# Inputs
sdf_parcels = spark.read.parquet(sf_parcels_out)
sdf_hedge = spark.read.parquet(sf_hedgerows_out)
sdf_woodland = spark.read.parquet(sf_nfi_out)

# COMMAND ----------


# Method
def group_agg_features(sdf, geom: str, group_cols: list):
    sdf = sdf.groupby(*group_cols).agg(
        F.expr(f"ST_MakeValid(ST_Union_Aggr({geom}))  AS geometry_tmp")
    )
    return sdf


def boundary_use(
    left,
    right,
    left_geom: str,
    right_geom: str,
    suffix: str,
    buf: int = 2,
    simp: int = 0,
    group_cols: list = ["major_grid", "bng_10km", "bng_1km", "id_parcel"],
):
    fn = """ST_MakeValid({opp}(
    {gb},
    ST_MakeValid(ST_Buffer(ST_SimplifyPreserveTopology({gtmp},{s}), {buf}))
    ))"""
    fn_in = fn.format(opp="ST_Intersection", gb=left_geom, gtmp="geometry_tmp", s=simp, buf=buf)
    fn_out = fn.format(opp="ST_Difference", gb=left_geom, gtmp="geometry_tmp", s=simp, buf=buf)

    right = group_agg_features(right, right_geom, group_cols)

    return (
        left.join(right, on=group_cols, how="left")
        .withColumn(
            "geometry_tmp", F.expr('COALESCE(geometry_tmp, ST_GeomFromText("POINT EMPTY"))')
        )
        .withColumn(f"geometry_{suffix}", F.expr(fn_in))
        .withColumn(left_geom, F.expr(fn_out))
        .drop("geometry_tmp")
    )


# COMMAND ----------

# DBTITLE 0,Priority method refactored
group_cols = ["major_grid", "bng_10km", "bng_1km", "id_parcel"]
sdf_woody_boundaries = (
    sdf_parcels.select(
        *group_cols, "geometry_boundary", F.expr("geometry_boundary AS geometry_boundary_orig")
    )
    .transform(
        boundary_use,
        sdf_hedge,
        left_geom="geometry_boundary",
        right_geom="geometry_hedge",
        suffix="hedgerow_boundary",
        simp=0,
        buf=2,
        group_cols=group_cols,
    )
    .transform(
        boundary_use,
        sdf_woodland,
        left_geom="geometry_boundary",
        right_geom="geometry_woodland",
        suffix="woodland_boundary",
        simp=0,
        buf=2,
        group_cols=group_cols,
    )
)

# Checkpoint. Record the boundary available. This is the geometry that will be converted into segments.
sdf_woody_boundaries = sdf_woody_boundaries.withColumn(
    "geometry_boundary_relict_available", F.col("geometry_boundary")
)

# Output
sdf_woody_boundaries.write.parquet(sf_woody_boundaries_out, mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Priortiy method - other woody features to segmented available parcel boundary

# COMMAND ----------


# DBTITLE 1,Segmentise the remaining boundary
@F.pandas_udf(T.ArrayType(T.BinaryType()))
def st_dump_to_list(s: pd.Series) -> pd.Series:
    """Converts a geoseries of geometries into a geoseries of lists of geometries. Multi part geometries are split into
    their components and single geometries are placed as a single entry in a list.
    """
    gs = gpd.GeoSeries.from_wkb(s)
    return gs.map(lambda g: [g.wkb for g in g.geoms] if hasattr(g, "geoms") else [g.wkb])


def st_interpolate_to_list(col: str, d: int = 10):
    @F.pandas_udf(T.ArrayType(T.BinaryType()))
    def udf(s: pd.Series) -> pd.Series:
        """Splits linestrings up based on distance. Splits line string into a list of line string segments of length approximately equal to input parameter d."""

        def linestring_interpolate(line):
            # generate the equidistant points
            distances = np.linspace(0, line.length, int(np.ceil(line.length / d)) + 1)[1:-1]
            points = (
                [list(line.boundary.geoms)[0]]
                + [line.interpolate(distance) for distance in distances]
                + [list(line.boundary.geoms)[1]]
            )
            lines = [LineString([a, b]).wkb for a, b in zip(points[:-1], points[1:])]
            return lines

        gs = gpd.GeoSeries.from_wkb(s)
        return gs.map(lambda l: linestring_interpolate(l))

    return udf(col)


def apply_st_func(sdf, func, explode_query, gcol, **kwargs):
    """Worker function to apply a function to the geometry column and explode the result.
    Explode query creates new id column so needs to be tailored to the function being applied.
    """
    (
        sdf.withColumn(gcol, F.expr(f"ST_AsBinary({gcol})"))
        .withColumn(gcol, func(gcol, **kwargs))
        .createOrReplaceTempView("tbl")
    )

    return spark.sql(explode_query)


def apply_st_dump(sdf, gcol="geometry"):
    explode = f"""select major_grid, bng_10km, bng_1km, id_parcel,
                ROW_NUMBER() over (partition by id_parcel ORDER BY id_parcel ASC) as id_boundary,
                EXPLODE({gcol}) as {gcol}
                from tbl
                """

    return apply_st_func(sdf, st_dump_to_list, explode, gcol)


def apply_st_interpolate(sdf, gcol="geometry", d=10):
    explode = f"""select major_grid, bng_10km, bng_1km, id_parcel, id_boundary,
                ROW_NUMBER() over (partition by id_parcel, id_boundary ORDER BY id_parcel, id_boundary ASC) as id_segment,
                EXPLODE({gcol}) as {gcol}
                from tbl
                """
    return apply_st_func(sdf, st_interpolate_to_list, explode, gcol, d=d)


#
# Create boundary segments
#
segment_query = (
    lambda t: f"""
select mp.major_grid, mp.bng_10km, mp.bng_1km, mp.id_parcel, mp.id_boundary, mp.id_segment, St_LineFromMultiPoint(St_collect(mp.p1, mp.p2)) as geometry
from
(
  select d.major_grid, d.bng_10km, d.bng_1km, d.id_parcel, d.id_boundary, d.id_segment, d.j p1, lead(d.j) over (partition by d.id_parcel, d.id_boundary order by d.id_segment) p2 from
  (
    select major_grid, bng_10km, bng_1km, id_parcel, id_boundary, EXPLODE(ST_DumpPoints(geometry_boundary_relict_available))  j, ROW_NUMBER() OVER (partition by id_parcel, id_boundary ORDER BY id_parcel, id_boundary ASC) AS id_segment from {t}
    ) d
) mp
where (p1 is not null) and (p2 is not null);
"""
)

# COMMAND ----------

sdf_woody_boundaries = spark.read.parquet(sf_woody_boundaries_out)

(
    sdf_woody_boundaries.withColumn(
        "geometry_boundary_relict_available",
        F.expr("ST_MakeValid(ST_SimplifyPreserveTopology(geometry_boundary_relict_available, 1))"),
    )
    .transform(apply_st_dump, gcol="geometry_boundary_relict_available")
    .withColumn(
        "geometry_boundary_relict_available", io.load_geometry("geometry_boundary_relict_available")
    )
    .createOrReplaceTempView("available_boundaries")
)

sdf_available_segments = (
    spark.sql(segment_query("available_boundaries"))
    .dropDuplicates(subset=["geometry"])
    .transform(apply_st_interpolate, gcol="geometry", d=20)  # using d=10 resulted in ~ 6x more rows
    # .withColumn("geometry", F.expr("ST_AsBinary(geometry)"))
)

sdf_available_segments.repartition(n1, "major_grid", "bng_10km").write.mode("overwrite").parquet(
    sf_available_segments_out
)

# COMMAND ----------

# DBTITLE 1,Buffer spatial join segments to other woody features
sdf_available_segments = spark.read.parquet(sf_available_segments_out).withColumn(
    "geometry", io.load_geometry("geometry")
)

# Other Woody - TOW
"""
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
"""

"""
# Load dissolved TOW data
sf_tow_dissolved_sp = "dbfs:/mnt/lab/unrestricted/elm/forest_research/tow_dissolved/TOW_SP_26062023.parquet"
sdf_tow = spark.read.parquet(sf_tow_dissolved_sp)

sdf_other_woody_tow = (
  st.sjoin(sdf_available_segments, sdf_tow, distance=BUFFER, lsuffix='_segjoin', rsuffix='_tow', spark=spark)
)
"""

# sdf_other_woody_tow.repartition(n2, "major_grid", "bng_10km").write.mode("overwrite").parquet(sf_other_woody_tow_out)

tow_class_cols = ["Woodland_Type_V2", "Avg_hight_m", "Shape_Area", "Shape_Length", "Average_Width"]

# COMMAND ----------

# Other Woody - VOM-TD
vom_class_cols = ["top_height"]

sdf_vom_td = (
    spark.read.parquet(sf_vom_td)
    .select(
        "top_x",
        "top_y",
        "top_height",  # int; tree top height
        "chm_path",
        "msg",
        "top_point",  # wkt; POINT representing tree top
        "crown_poly_raster",  # wkt; POLYGON representing tree crown
    )
    .withColumn(
        "geometry", io.load_geometry(column="top_point", encoding_fn="ST_GeomFromText")
    )  # Use coordinate to join to parcels for efficiency
)
sdf_other_woody_vom = st.sjoin(
    sdf_available_segments,
    sdf_vom_td,
    distance=BUFFER,
    lsuffix="_segjoin",
    rsuffix="_vom_td",
    spark=spark,
)

sdf_other_woody_vom.repartition(n2, "major_grid", "bng_10km").write.mode("overwrite").parquet(
    sf_other_woody_vom_out
)

# COMMAND ----------

# DBTITLE 1,Priority method
"""
sdf_other_woody_tow_relict = (spark.read.parquet(sf_other_woody_tow_out)
                              .filter(F.expr( "Woodland_Type_V2 not in ('Small woods', 'Group of trees')"))
)
"""

sdf_other_woody_vom = spark.read.parquet(sf_other_woody_vom_out).withColumn(
    "geometry", io.load_geometry("crown_poly_raster", encoding_fn="ST_GeomFromText")
)  # use crown polygon when intersecting with boundary

# Priority method other woody
group_cols = ["major_grid", "bng_10km", "bng_1km", "id_parcel", "id_boundary", "id_segment"]
sdf_other_woody_segments = (
    sdf_available_segments.select(
        *group_cols,
        F.expr("geometry AS geometry_segment"),
        F.expr("geometry AS geometry_segment_orig"),
    )
    # .transform(boundary_use,
    #           sdf_other_woody_tow_relict.select(*group_cols, 'geometry_tow'),
    #           left_geom = 'geometry_segment',
    #           right_geom = 'geometry_tow',
    #           suffix='other_woody_tow_boundary_relict',
    #           simp = 2,
    #           buf = 2,
    #           group_cols = group_cols)
    .transform(
        boundary_use,
        sdf_other_woody_vom.select(*group_cols, "geometry_vom_td"),
        left_geom="geometry_segment",
        right_geom="geometry_vom_td",
        suffix="other_woody_vom_boundary",
        simp=2,
        buf=2,
        group_cols=group_cols,
    )
)

# Taking many hours why segments split into 10m chunks
# Also takes about 48s including tow in the priority use, but only 2s with only vom (without restrictions on segment length)
# takes about 25s including tow in the priority use, but only 2s with only vom (with 50m restriction on segment length)
sdf_other_woody_segments.repartition(n2, "major_grid", "bng_10km").write.mode("overwrite").parquet(
    sf_other_woody_segments_out
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Classify segments as relict hedge and join back to original boundaries

# COMMAND ----------

# DBTITLE 1,Define Relict Hedgerow from Other Woody
# Select just segments which are intersected by some other woody geometry and calculate the length of intersection
spark.read.parquet(sf_other_woody_segments_out).createOrReplaceTempView("woody_segments")
sdf_relict_segments = spark.sql(
    """
                                select *,
                                length_other_woody_vom + length_other_woody_tow as total_other_woody_length,
                                length_other_woody_vom / segment_length as prop_vom,
                                length_other_woody_tow / segment_length as prop_tow,
                                (length_other_woody_vom + length_other_woody_tow) / segment_length as prop_all
                                from (
                                select
                                major_grid,
                                bng_10km,
                                bng_1km,
                                id_parcel,
                                id_boundary,
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
                                group by major_grid, bng_10km, bng_1km, id_parcel, id_boundary, id_segment, geometry_segment_orig ) tbl2;
                                """
)

sdf_relict_segments.repartition(n2, "major_grid", "bng_10km").write.mode("overwrite").parquet(
    sf_relict_segments_out
)

# COMMAND ----------

# DBTITLE 1,Join back to available parcel geometries
# Use length and class fields to classify segments as relict
sdf_relict_segments = (
    spark.read.parquet(sf_relict_segments_out)
    .withColumnRenamed("geometry", "geometry_relict")
    .filter(F.expr("ST_Length(geometry)>2"))
    .filter(F.col("prop_all") > 0.3)
)

# Run priority method on the boundary available to be relict
sdf_woody_boundaries = spark.read.parquet(sf_woody_boundaries_out).withColumn(
    "geometry_boundary", F.col("geometry_boundary_relict_available")
)

# Priority method with relict hedge
sdf_woody_boundaries = sdf_woody_boundaries.transform(
    boundary_use,
    sdf_relict_segments,
    left_geom="geometry_boundary",
    right_geom="geometry_relict",
    suffix="relict_boundary",
    simp=0,
    buf=2,
)
sdf_woody_boundaries.repartition(n2, "major_grid", "bng_10km").write.mode("overwrite").parquet(
    sf_woody_boundaries_out
)

# COMMAND ----------

sf_woody_boundaries_out
