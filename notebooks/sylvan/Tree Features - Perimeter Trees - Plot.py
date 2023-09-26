# Databricks notebook source
# MAGIC %pip install rich
# MAGIC %pip install contextily

# COMMAND ----------

import os
import sys
import re
from itertools import chain
from typing import List, Optional, Iterator, Tuple

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
from shapely import from_wkt

from log import LOG
from joins import spatial_join
from tree_features_old import *

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, StringType
from sedona.register import SedonaRegistrator

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib as mpl
import contextily as ctx

from matplotlib.ticker import FuncFormatter, PercentFormatter

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
SedonaRegistrator.registerAll(spark)

# COMMAND ----------

parcel_buffer_distance=2

timestamp = "202308040848"

hedgerows_path = "dbfs:/mnt/lab/unrestricted/elm_data/rural_payments_agency/efa_hedges/2022_06_24.parquet"

hedges_length_path = "dbfs:/mnt/lab/unrestricted/elm/elmo/hedgerows_and_water/hedgerows_and_water.csv"

parcels_path = "dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_02_07.parquet"

path_output_template = ( "dbfs:/mnt/lab/unrestricted/elm/elmo/"
                "hrtrees/tree_detections/"
                "tree_detections_{timestamp}.parquet"
            )

hrtrees_output_template = ( "dbfs:/mnt/lab/unrestricted/elm/elmo/"
                "hrtrees/hrtrees_{timestamp}.parquet"
            )

parcel_hrtrees_template = ( "dbfs:/mnt/lab/unrestricted/elm/elmo/"
                "hrtrees/parel_hrtrees_count_{timestamp}.parquet"
            )

#output_trees_path = path_output_template.format(mtc='SP_SU', timestamp=timestamp)
#output_trees_path= path_output_template.format(mtc="SP65", timestamp="0000")
output_trees_path= path_output_template.format(timestamp=timestamp)
hrtrees_path_output = hrtrees_output_template.format(timestamp=timestamp)
parcel_hrtrees_output = parcel_hrtrees_template.format(timestamp=timestamp)

output_hrtrees_per_parcel_path =  "/dbfs/mnt/lab/unrestricted/elm/elmo/hrtrees/hrtrees_per_parcel.csv"

tile_to_visualise = "SP65nw"

# COMMAND ----------

gcols = ['parcel_geom', 'perimeter_geom', 'perimeter_geom_buf', 'interior_geom', 'top_point', 'crown_poly_raster', 'geom_right', 'geom_left', "crown_perim_intersection"]

def make_geoms(df, gcols):
    for c in gcols:
        if c in df.columns:
            try:
                df[c] = df[c].map(lambda g: from_wkt(g))
            except TypeError:
                continue
    return df

def toPandasGeoToText(df, gcols):
    for c in gcols:
        if c in df.columns:
            df = df.withColumn(c, F.expr(f"ST_AsText({c})"))
    
    dfp = df.toPandas()

    return make_geoms(dfp, gcols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data

# COMMAND ----------

treesDF = spark.read.parquet(output_trees_path)

# COMMAND ----------

parcelsDF = spark.read.parquet(parcels_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Select just one tile to test methods with

# COMMAND ----------

treesDF = treesDF.filter(f"chm_path like '%{tile_to_visualise}%'")
parcelsDF = parcelsDF.filter(f"SHEET_ID like '{tile_to_visualise[:2]}%'")

# COMMAND ----------

'''
output_trees_path = path_output_template.format(mtc='SP_SU', timestamp=timestamp)
treesDFAlt = spark.read.parquet(output_trees_path)
treesDFAlt = treesDFAlt.filter(f"chm_path like '%{tile_to_visualise}%'")
treesDFAlt.count()
'''

# COMMAND ----------

treesDF.count(), treesDF.select("crown_poly_raster").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Test perimeter trees feature

# COMMAND ----------

parcelsDF = parcelsDF.withColumn("SHEET_PARCEL_ID", F.concat("SHEET_ID","PARCEL_ID"))

non_geo_cols = parcelsDF.columns

parcelsDF = parcelsDF.withColumn("parcel_geom", F.expr("ST_MakeValid(ST_GeomFromWKB(wkb_geometry))"))
parcelsDF = parcelsDF.withColumn("perimeter_geom", F.expr("ST_MakeValid(ST_Boundary(parcel_geom))"))
parcelsDF = parcelsDF.withColumn("perimeter_length", F.expr("ST_Length(perimeter_geom)"))

parcel_buffer_distance=2
parcelsDF = parcelsDF.withColumn("perimeter_geom_buf", F.expr(f"ST_Buffer(perimeter_geom,{parcel_buffer_distance})"))
parcelsDF = parcelsDF.withColumn("interior_geom", F.expr("ST_MakeValid(ST_Difference(parcel_geom, perimeter_geom_buf))"))

# COMMAND ----------

parcelsDF.count()

# COMMAND ----------

df_parcels = (parcelsDF
              .filter(F.col("SHEET_PARCEL_ID")=="SP37379069")
)
display(df_parcels)

# COMMAND ----------

df_parcels = toPandasGeoToText(df_parcels, gcols)
df_parcels

# COMMAND ----------

dfplot = df_parcels.loc[ df_parcels['SHEET_PARCEL_ID']=="SP37379069"]
dfplot = gpd.GeoDataFrame(dfplot, geometry='parcel_geom')

f, ax = plt.subplots(figsize = (10,10))

dfplot['parcel_geom'].plot(ax=ax, facecolor='blue', edgecolor='k', linewidth=0.5, alpha=0.7)
dfplot.set_geometry('perimeter_geom_buf').plot(ax=ax, facecolor='green', edgecolor='k', linewidth=0.5, alpha=0.7)
dfplot.set_geometry('interior_geom').plot(ax=ax, facecolor='yellow', edgecolor='k', linewidth=0.5, alpha=0.7)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Test crowns match coords after join

# COMMAND ----------

treesDF = treesDF.withColumn("geometry", F.expr("ST_Point(top_x, top_y)"))

# COMMAND ----------

crowns = treesDF.select("crown_poly_raster").withColumn("geometry", F.expr("ST_GeomFromWKT(crown_poly_raster)"))
treesPointsDF = treesDF.drop("crown_poly_raster")

treesAlignedDF = spatial_join(
                    crowns,
                    treesPointsDF,
                    spark,
                    partitioning = None,
                    partition_right = False,
                    useIndex = True,
                    considerBoundaryIntersection = False, # returns tree geometries contained within parcel geoms (since trees are point geoms using 'True' should produce the result)
                    calculate_proportion = False
)

# COMMAND ----------

# Filter to just select geoms in a bounding box
xmin=460650
xmax=460800
ymin=256950
ymax=257100

# Define a bounding box to filter data by
bbox_coords = [(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax), (xmin, ymin) ]
bbox = Polygon(bbox_coords)
gdfbb = gpd.GeoDataFrame({'geometry':[bbox]}, crs='EPSG:27700').to_crs("EPSG:3857")

spark.sql(f"select ST_GeomFromWKT('{bbox.wkt}')")

# COMMAND ----------

treesDFSub = treesDF.filter(F.expr(f"""
                                ST_Contains(
                                    ST_GeomFromWKT('{bbox.wkt}'),geometry
                                )
                                """)
                         )

parcelsDFSub = parcelsDF.filter(F.expr(f"""
                                ST_Contains(
                                    ST_GeomFromWKT('{bbox.wkt}'),parcel_geom
                                )
                                """)
                         )

df_trees = toPandasGeoToText(treesDFSub, ['geometry'])
df_parcels = toPandasGeoToText(parcelsDFSub, gcols)

#df_trees = treesDFSub.toPandas()
#df_parcels = parcelsDFSub.toPandas()


df_trees.shape, df_parcels.shape

# COMMAND ----------

'''
gcols = ['parcel_geom', 'perimeter_geom', 'perimeter_geom_buf', 'interior_geom', 'top_point', 'crown_poly_raster', 'geom_right', 'geom_left']
#gcols = ['geom_left', 'geom_right']
for c in gcols:
    if c in gdf.columns:
        try:
            gdf[c] = gdf[c].map(lambda g: from_wkt(g))
        except TypeError:
            continue

gdf = gpd.GeoDataFrame(gdf, geometry='perimeter_geom_buf')

# COMMAND ----------

df_trees = make_geoms(df_trees, gcols)
gdf_trees = gpd.GeoDataFrame(df_trees, geometry='geometry')
gdf_trees.head()

# COMMAND ----------

# Check alignment between tree points and crowns
f, ax = plt.subplots(figsize = (12,12))

#gdf_trees.plot(ax=ax, edgecolor='k', linewidth=0.5)
gdf_trees.set_geometry('crown_poly_raster').plot(ax=ax, facecolor='grey', edgecolor='k', linewidth=0.5, alpha=0.7)


#ax.set_xlim(xmin = 460650, xmax=460800)
#ax.set_ylim(ymin = 256950, ymax=257100)


# COMMAND ----------

# Check alignment between tree points and crowns
f, ax = plt.subplots(figsize = (12,12))

gdf_trees.set_geometry('top_point').plot(ax=ax, edgecolor='k', linewidth=0.5)
gdf_trees.set_geometry('crown_poly_raster').plot(ax=ax, facecolor='grey', edgecolor='k', linewidth=0.5, alpha=0.7)


#ax.set_xlim(xmin = 460650, xmax=460800)
#ax.set_ylim(ymin = 256950, ymax=257100)

# COMMAND ----------

gdf_trees.apply(lambda row: row['crown_poly_raster'].contains(row['top_point']), axis=1).all()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Check spatial join between trees and parcels and the subsequent spatial operations

# COMMAND ----------

treesDF.schema

# COMMAND ----------

# Create geometries
parcelsDF = make_parcel_geometries(parcelsDF, parcel_buffer_distance)
treesDF = treesDF.withColumn("top_point", F.expr("ST_GeomFromWKT(top_point)"))

# Create unique parcel id
parcelsDF = parcelsDF.withColumn("SHEET_PARCEL_ID", F.concat("SHEET_ID","PARCEL_ID"))

# COMMAND ----------

treesDF = treesDF.withColumn("geometry", F.col("top_point"))
parcelsDF = parcelsDF.withColumn("geometry", F.col("perimeter_geom_buf"))

# COMMAND ----------

# Join trees with buffered percel perimeter
pTreesDF = spatial_join(
                parcelsDF,
                treesDF,
                spark,
                partitioning = None,
                partition_right = False,
                useIndex = True,
                considerBoundaryIntersection = False, # returns tree geometries contained within parcel geoms (since trees are point geoms using 'True' should produce the result)
                calculate_proportion = False
)

# COMMAND ----------

# Get the length of the perimeter that is intersected by tree crowns
perimTreesIntDF = pTreesDF.withColumn("crown_perim_intersection", F.expr("""
                                                            ST_Intersection(
                                                                ST_GeomFromWKT(crown_poly_raster), ST_GeomFromWKT(perimeter_geom)
                                                            )
                                                            """)
                                )

perimTreesIntDF = perimTreesIntDF.withColumn("crown_perim_length", F.expr("""
                                                            ST_Length(crown_perim_intersection)
                                                            """)
                                )

# COMMAND ----------

df = perimTreesIntDF.toPandas()
gdf = gpd.GeoDataFrame(df)

# COMMAND ----------

# Create geometries
gcols = ['parcel_geom', 'perimeter_geom', 'perimeter_geom_buf', 'interior_geom', 'top_point', 'crown_poly_raster', 'geom_right', 'geom_left', "crown_perim_intersection"]
#gcols = ['geom_left', 'geom_right']

gdf = make_geoms(gdf, gcols)

# COMMAND ----------

f, ax = plt.subplots(figsize = (15,15))

gdf.set_geometry('perimeter_geom_buf').plot(ax=ax, facecolor='lightblue', edgecolor='k', linewidth=0.5, alpha=0.7)
gdf.set_geometry('perimeter_geom').plot(ax=ax, edgecolor='red', linewidth=1.5, alpha=1)

gdf.set_geometry('crown_perim_intersection').plot(ax=ax, edgecolor='green', linewidth=1.5, alpha=1)

gdf.set_geometry('crown_poly_raster').plot(ax=ax, facecolor='grey', edgecolor='k', linewidth=0.5, alpha=0.7)
gdf.set_geometry('top_point').plot(ax=ax, edgecolor='k', linewidth=0.5)

ax.set_xlim(xmin = 460650, xmax=460800)
ax.set_ylim(ymin = 256950, ymax=257100)

# COMMAND ----------

perimTreesIntDF.columns

# COMMAND ----------

DF = (perimTreesIntDF
            .select("SHEET_PARCEL_ID", "geom_right", "crown_perim_length")
            .distinct()
            .groupby("SHEET_PARCEL_ID")
            .agg(F.count("geom_right").alias("perim_trees_count"),
                F.sum("crown_perim_length").alias("perim_trees_length"),
                )
)

# COMMAND ----------

# Aggregate and get stats
df_results = DF.toPandas()

# COMMAND ----------

df_results[["perim_trees_length", "perim_trees_length"]].describe()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Method to get features
# MAGIC
# MAGIC Compare their results to the step-by-step appraoch used above

# COMMAND ----------

treesDF = spark.read.parquet(output_trees_path)
parcelsDF = spark.read.parquet(parcels_path)

treesDF = treesDF.filter(f"chm_path like '%{tile_to_visualise}%'")
parcelsDF = parcelsDF.filter(f"SHEET_ID like '{tile_to_visualise[:2]}%'")

parcelsDF = make_parcel_geometries(parcelsDF, parcel_buffer_distance)
treesDF = treesDF.withColumn("top_point", F.expr("ST_GeomFromWKT(top_point)"))

# Create unique parcel id
parcelsDF = parcelsDF.withColumn("SHEET_PARCEL_ID", F.concat("SHEET_ID","PARCEL_ID"))

# COMMAND ----------

parcelTreesFeaturesDF = get_perimeter_trees_features(spark,
                                                    treesDF,
                                                    parcelsDF
                                                    )

# COMMAND ----------

df_results_func = parcelTreesFeaturesDF.toPandas()

# COMMAND ----------

df_results_func.head()

# COMMAND ----------

data = df_results_func.iloc[ np.random.choice(df_results_func.index, 6)].values

f, axs = plt.subplots(2,3,figsize=(40,30))
axs = axs.reshape(1,-1)[0]
for i, (parcel_id, ntrees, length) in enumerate(data):
    ax=axs[i]

    df_parcels = (parcelsDF
              .filter(F.col("SHEET_PARCEL_ID")==parcel_id)
              .toPandas()
    )
    df_parcels = make_geoms(df_parcels, gcols)
    gdf_parcels = gpd.GeoDataFrame(df_parcels, geometry='perimeter_geom')
    xmin,ymin,xmax,ymax = gdf_parcels.total_bounds

    bbox_coords = [(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax), (xmin, ymin) ]
    bbox = Polygon(bbox_coords)
    gdfbbox = gpd.GeoDataFrame({'geometry':[bbox]}, geometry="geometry")

    df_trees = (treesDF
                .filter(F.expr(f"""
                                ST_Contains(
                                    ST_GeomFromWKT('{bbox.wkt}'),top_point
                                )
                                """))
                .toPandas()
                )
    df_trees = make_geoms(df_trees, gcols)
    gdf_trees = gpd.GeoDataFrame(df_trees, geometry='top_point')

    gdf_parcels.set_geometry('perimeter_geom_buf').plot(ax=ax, facecolor='lightblue', edgecolor=None, linewidth=0.5, alpha=0.7)
    gdf_parcels.set_geometry('perimeter_geom').plot(ax=ax, edgecolor='red', linewidth=1.0, alpha=1)

    gdf_trees.set_geometry('crown_poly_raster').plot(ax=ax, facecolor='grey', edgecolor=None, linewidth=0.5, alpha=0.7)
    gdf_trees.set_geometry('top_point').plot(ax=ax, edgecolor=None, linewidth=0.5)

    gdf_trees_int = gdf_trees.loc[ gdf_trees['top_point'].map(lambda g: g.intersects(gdf_parcels.perimeter_geom_buf.values[0]))]
    gdf_trees_int.set_geometry('top_point').plot(ax=ax, facecolor='lime')

    # Calculate crown intersection to compare total length of intersection
    gdf_trees_int['crown_perim_intersection'] = gdf_trees_int.apply(lambda row: row['crown_poly_raster'].intersection(gdf_parcels.perimeter_geom),axis=1)
    gdf_trees_int['intersection_length'] = gdf_trees_int['crown_perim_intersection'].map(lambda g: g.length)


    gdf_trees_int.set_geometry('crown_perim_intersection').plot(ax=ax, edgecolor='lime', linewidth=1.5, alpha=1)

    ax.set_axis_off()
    ax.set_title(f"{ntrees} trees in parcel perimeter\n{length:.2f},{gdf_trees_int.intersection_length.sum():.2f} perim tree cover")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Checking interior trees calculation

# COMMAND ----------

treesDF = spark.read.parquet(output_trees_path)
parcelsDF = spark.read.parquet(parcels_path)

treesDF = treesDF.filter(f"chm_path like '%{tile_to_visualise}%'")
parcelsDF = parcelsDF.filter(f"SHEET_ID like '{tile_to_visualise[:2]}%'")

parcelsDF = make_parcel_geometries(parcelsDF, parcel_buffer_distance)
treesDF = treesDF.withColumn("top_point", F.expr("ST_GeomFromWKT(top_point)"))

# Create unique parcel id
parcelsDF = parcelsDF.withColumn("SHEET_PARCEL_ID", F.concat("SHEET_ID","PARCEL_ID"))

# COMMAND ----------

interiorTreesFeaturesDF = get_interior_trees_features(spark, treesDF, parcelsDF)

# COMMAND ----------

df_int_results = interiorTreesFeaturesDF.toPandas()

# COMMAND ----------

'''
parcelsDF = parcelsDF.withColumn("geometry", F.col("interior_geom")) # parcel interior, with buffered perimeter removed
treesDF = treesDF.withColumn("geometry", F.col("top_point")) # tree top coordinate

iTreesDF = spatial_join(
                parcelsDF,
                treesDF,
                spark,
                partitioning = None,
                partition_right = False,
                useIndex = True,
                considerBoundaryIntersection = False, # returns tree geometries contained within parcel geoms (since trees are point geoms using 'True' should produce the result)
                calculate_proportion = False
)

df_iTreesDF = iTreesDF.toPandas()
gdf_int_results = gpd.GeoDataFrame(df_iTreesDF)
gdf_int_results = make_geoms(gdf_int_results, gcols)
'''

# COMMAND ----------

data = df_int_results.iloc[ np.random.choice(df_int_results.index, 1)].values

f, ax = plt.subplots(figsize=(12,12))
for i, (parcel_id, ntrees) in enumerate(data):

    df_parcels = (parcelsDF
              .filter(F.col("SHEET_PARCEL_ID")==parcel_id)
              .toPandas()
    )
    df_parcels = make_geoms(df_parcels, gcols)
    gdf_parcels = gpd.GeoDataFrame(df_parcels, geometry='perimeter_geom')
    xmin,ymin,xmax,ymax = gdf_parcels.total_bounds

    bbox_coords = [(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax), (xmin, ymin) ]
    bbox = Polygon(bbox_coords)
    gdfbbox = gpd.GeoDataFrame({'geometry':[bbox]}, geometry="geometry")

    df_trees = (treesDF
                .filter(F.expr(f"""
                                ST_Contains(
                                    ST_GeomFromWKT('{bbox.wkt}'),top_point
                                )
                                """))
                .toPandas()
                )
    df_trees = make_geoms(df_trees, gcols)
    gdf_trees = gpd.GeoDataFrame(df_trees, geometry='top_point')

    gdf_parcels.set_geometry('interior_geom').plot(ax=ax, facecolor='lightblue', edgecolor=None, linewidth=0.5, alpha=0.7)
    gdf_parcels.set_geometry('perimeter_geom').plot(ax=ax, edgecolor='red', linewidth=1.0, alpha=1)

    gdf_trees.set_geometry('crown_poly_raster').plot(ax=ax, facecolor='grey', edgecolor=None, linewidth=0.5, alpha=0.7)
    gdf_trees.set_geometry('top_point').plot(ax=ax, edgecolor=None, linewidth=0.5)

    gdf_trees_int = gdf_trees.loc[ gdf_trees['top_point'].map(lambda g: g.intersects(gdf_parcels.interior_geom.values[0]))]
    gdf_trees_int.set_geometry('top_point').plot(ax=ax, facecolor='lime')

    ax.set_axis_off()
    ax.set_title(f"{ntrees} trees in parcel interior")

# COMMAND ----------

data = df_int_results.iloc[ np.random.choice(df_int_results.index, 6)].values

f, axs = plt.subplots(2,3,figsize=(40,30))
axs = axs.reshape(1,-1)[0]
for i, (parcel_id, ntrees) in enumerate(data):
    ax=axs[i]

    df_parcels = (parcelsDF
              .filter(F.col("SHEET_PARCEL_ID")==parcel_id)
              .toPandas()
    )
    df_parcels = make_geoms(df_parcels, gcols)
    gdf_parcels = gpd.GeoDataFrame(df_parcels, geometry='perimeter_geom')
    xmin,ymin,xmax,ymax = gdf_parcels.total_bounds

    bbox_coords = [(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax), (xmin, ymin) ]
    bbox = Polygon(bbox_coords)
    gdfbbox = gpd.GeoDataFrame({'geometry':[bbox]}, geometry="geometry")

    df_trees = (treesDF
                .filter(F.expr(f"""
                                ST_Contains(
                                    ST_GeomFromWKT('{bbox.wkt}'),top_point
                                )
                                """))
                .toPandas()
                )
    df_trees = make_geoms(df_trees, gcols)
    gdf_trees = gpd.GeoDataFrame(df_trees, geometry='top_point')

    gdf_parcels.set_geometry('interior_geom').plot(ax=ax, facecolor='lightblue', edgecolor=None, linewidth=0.5, alpha=0.7)
    gdf_parcels.set_geometry('perimeter_geom').plot(ax=ax, edgecolor='red', linewidth=1.0, alpha=1)

    gdf_trees.set_geometry('crown_poly_raster').plot(ax=ax, facecolor='grey', edgecolor=None, linewidth=0.5, alpha=0.7)
    gdf_trees.set_geometry('top_point').plot(ax=ax, edgecolor=None, linewidth=0.5)

    gdf_trees_int = gdf_trees.loc[ gdf_trees['top_point'].map(lambda g: g.intersects(gdf_parcels.interior_geom.values[0]))]
    gdf_trees_int.set_geometry('top_point').plot(ax=ax, facecolor='lime')

    ax.set_axis_off()
    ax.set_title(f"{ntrees} trees in parcel interior")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Test getting all parcel tree features
# MAGIC

# COMMAND ----------

treesDF = spark.read.parquet(output_trees_path)
parcelsDF = spark.read.parquet(parcels_path)

treesDF = treesDF.filter(f"chm_path like '%{tile_to_visualise}%'")
parcelsDF = parcelsDF.filter(f"SHEET_ID like '{tile_to_visualise[:2]}%'")


# COMMAND ----------

parcelTreeFeaturesDF = get_parcel_perimeter_and_interior_tree_features(spark,
                                                                       treesDF,
                                                                       parcelsDF,
                                                                       parcel_buffer_distance,
                                                                       double_count=False
                                                                       )


# COMMAND ----------

df_ptrees = parcelTreeFeaturesDF.toPandas()

# COMMAND ----------

df_ptrees.head()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Riparian Trees

# COMMAND ----------


waterbodies_path = "dbfs:/mnt/lab/unrestricted/elm/buffer_strips/waterbody_geometries.parquet"

# COMMAND ----------

wbDF = spark.read.parquet(waterbodies_path)

# COMMAND ----------

# Understand categories that are used
wbDF.createOrReplaceTempView("waterbodies")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select theme, count(*)
# MAGIC from waterbodies
# MAGIC group by theme

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from 
# MAGIC (select description, count(*)
# MAGIC from waterbodies
# MAGIC group by description) as d

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select watertype, count(*)
# MAGIC from waterbodies
# MAGIC group by watertype

# COMMAND ----------

# filter waterbodies data
wbDF = wbDF.filter(f"id_parcel like '{tile_to_visualise[:4]}%'")

# COMMAND ----------

display(wbDF)

# COMMAND ----------

# Create geometries
river_buffer_distance = 2
wbDF = wbDF.withColumn("geometry_water_buffered", F.expr(f"ST_Buffer(geometry_water, {river_buffer_distance})"))

wbDF = wbDF.withColumn("SHEET_PAREL_ID", F.col("id_parcel"))

# COMMAND ----------

# Select only waterbodies of interest
waterbodies_exclude = ['Sea']
wbDF = wbDF.filter(f"""description not in ({",".join("'{}'".format(i) for i in waterbodies_exclude)})""")

# COMMAND ----------

wbDF.count()

# COMMAND ----------

#treesDF = treesDF.withColumn("top_point", F.expr("ST_GeomFromWKT(top_point)"))
treesDF = treesDF.withColumn("geometry", F.col("top_point")) # tree top coordinate
wbDF = wbDF.withColumn("geometry", F.col("geometry_water_buffered")) # buffered hedge linestring

wbTreesDF, wbTreesPerParcelDF, counts = get_waterbody_trees_features(spark,
                                                                    treesDF,
                                                                    wbDF,
                                                                    double_count=True)

# COMMAND ----------

# Test refactored method by running hedgerows intersection
hedgerows_path = "dbfs:/mnt/lab/unrestricted/elm_data/rural_payments_agency/efa_hedges/2022_06_24.parquet"
hrDF = spark.read.parquet(hedgerows_path)
hrDF = hrDF.filter(f"REF_PARCEL_SHEET_ID like '{tile_to_visualise[:2]}%'")
hrDF = hrDF.withColumn('SHEET_PARCEL_ID', F.concat("REF_PARCEL_SHEET_ID", "REF_PARCEL_PARCEL_ID"))

hedgerow_buffer_distance=2
hrDF = (hrDF
        .withColumn("wkb" , F.col("geometry"))
        .withColumn("buffered_geom", F.expr(f"ST_Buffer(ST_GeomFromWKB(wkb), {hedgerow_buffer_distance})"))
)

treesDF = treesDF.withColumn("top_point", F.expr("ST_GeomFromWKT(top_point)"))

hrTreesDF, hrTreesPerParcelDF, counts = get_hedgerow_trees_features(spark,
                                                                    treesDF,
                                                                    hrDF,
                                                                    double_count=True
                                                                    )

# COMMAND ----------

counts

# COMMAND ----------


