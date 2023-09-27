# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Relict Hedges - Figures
# MAGIC
# MAGIC Enrich hedgerow data by identifying relict hedges
# MAGIC
# MAGIC Create plots to show the relict hedge classification methodology and results.

# COMMAND ----------

# MAGIC %pip install contextily

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd
import shapely
import geopandas as gpd
import contextily as ctx
from shapely.geometry import Polygon
from shapely import from_wkt, from_wkb
from typing import Tuple

from elmo_geo import register
from elmo_geo.st import st
from elmo_geo.io import io2 as io
from elmo_geo.utils import types

from pyspark.sql import functions as F

register()

# COMMAND ----------

def setup_geoplot(lims = (446450, 349550, 446650, 349850), basemap = True, figsize = (16,9)):
  fig, ax = plt.subplots(figsize=figsize)
  ax.axis('off')
  if lims is not None:
      ax.set(xlim=[lims[0], lims[2]], ylim=[lims[1], lims[3]])
  if basemap:
      ctx.add_basemap(ax=ax, source=ctx.providers.Thunderforest.Landscape(apikey='25a2eb26caa6466ebc5c2ddd50c5dde8', attribution=None), crs='EPSG:27700')
  return fig, ax

def add_patch(legend, layers):
    from matplotlib.patches import Patch
    ax = legend.axes

    handles, labels = ax.get_legend_handles_labels()
    for n,c in layers:
        handles.append(Patch(facecolor=c, edgecolor=c))
        labels.append(n)

    legend._legend_box = None
    legend._init_legend_box(handles, labels)
    legend._set_loc(legend._loc)
    legend.set_title(legend.get_title().get_text())

def layers_plot(*layers, lims, figsize = (15,15), suptitle = "", axtitle = "", stagger = True, basemap=False, **kwargs):
    '''
    Each element is layers is a 4-tuple of:
    - gdf: the data to be plotted
    - name: the name of the layer
    - color: the color to plot the layer
    - kwargs: dict containing any other keyword arguments to pass to the plot() function
    '''
    layers = list(layers)
    n_layers = len(layers)
    figures = []
    for i in range(n_layers):

        if stagger==False:
            # Only produce the final scene with all layers
            if i < n_layers-1:
                continue

        f, ax = setup_geoplot(lims = lims, basemap=basemap, figsize = figsize)

        additional_legend_patches = []

        # Plot each layer up to i
        for j in range(i+1):
            alpha = 1
            if j<i:
                alpha = 0.5
            gdf, name, color, kwargs = layers[j]
            # drop empty geometries
            gdf = gdf.loc[~gdf.is_empty]
            gdf.plot(ax=ax, label = name, color = color, alpha=alpha, zorder = j, **kwargs)

            # For some reason polygon layers do not appeach in the legend automatically. Need to add patches manually
            if 'Polygon' in gdf.geometry.type.unique():
                additional_legend_patches.append((name, color))

        ax.legend(fontsize = 20, loc = 'lower right')
        add_patch(ax.get_legend(), layers = additional_legend_patches)
        plt.suptitle(suptitle, fontsize = 22, y = 0.879, color = 'black', fontweight='normal', backgroundcolor='white')
        ax.set_title(axtitle, fontsize = 20, y = 0.942, x = 0.485, color = 'black', fontweight = 'normal', backgroundcolor = 'white')

        figures.append(f)
    return figures

def load_paths(subdir:str, datasets:str = ['parcels','parcel_segments','hedgerows','woodland','other_woody_tow','other_woody_vom_td','woody_boundaries','boundary_uses','relict_segments'])->dict:
    template = 'dbfs:/mnt/lab/unrestricted/elm/elm_se/{subdir}/elm_se-{dataset}-2023_08.parquet'
    paths = {}
    for ds in datasets:
        key = f"sf_{ds}_out"
        paths[key] = template.format(subdir=subdir, dataset = ds)
    return paths

def load_data(subdir:str, datasets:list)->Tuple[types.SparkDataFrame]:
    paths_out = load_paths(subdir = subdir, datasets= datasets)
    output = []
    for ds in datasets:
        key = f"sf_{ds}_out"
        sdf = spark.read.parquet(paths_out[key])
        if ds == 'hedgerows':
            sdf = (sdf
            .withColumn("length", F.expr("ST_Length(geometry_hedge)"))
            .withColumn("major_grid", F.expr("LEFT(bng_10km, 2)"))
            )
        output.append(sdf)

    return output

# COMMAND ----------

# Input data paths
sf_vom_td = f"dbfs:/mnt/lab/unrestricted/elm/elmo/tree_features/tree_detections/tree_detections_202308040848.parquet"
sf_tow_sp = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_SP_England_26062023.parquet"
sf_tow_lidar = "dbfs:/mnt/lab/unrestricted/elm_data/forest_research/TOW_LiDAR_England_26062023.parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Test boundary segment method

# COMMAND ----------

parcels_to_test = ["SP62553568", "SP62553755", "SP62555066"]

# COMMAND ----------

sdf_parcel_sub = (spark.read.parquet(sf_parcel)
                  .filter(F.col("id_parcel").isin(parcels_to_test))
)
sdf_parcel_sub.count()

# COMMAND ----------

segment_query = """select mp.id_parcel, mp.rn, St_LineFromMultiPoint(St_collect(mp.p1, mp.p2)) as boundary_segment
from
(
  select id_parcel, d.rn, d.j p1, lead(d.j) over (partition by d.id_parcel order by d.rn) p2 from
  (
    select id_parcel, EXPLODE(ST_DumpPoints(geometry_boundary))  j, ROW_NUMBER() OVER (ORDER BY id_parcel ASC) AS rn from ps
    ) d
) mp
where (p1 is not null) and (p2 is not null)"""

# COMMAND ----------

sdf_parcel_sub.createOrReplaceTempView("ps")
df_seg = spark.sql(segment_query)
df_seg = df_seg.withColumn("boundary_segment", F.expr("ST_AsText(boundary_segment)"))

# COMMAND ----------

gdf_seg = df_seg.toPandas()
gdf_seg['geometry'] = gdf_seg["boundary_segment"].map(lambda x: from_wkt(x))
gdf_seg = gpd.GeoDataFrame(gdf_seg, geometry='geometry')

# COMMAND ----------

import matplotlib as mpl

f, ax = plt.subplots(figsize = (10,10))
for pid in gdf_seg["id_parcel"].unique():
    sub = gdf_seg.loc[ gdf_seg['id_parcel']==pid]
    norm = mpl.colors.Normalize(vmin=min(sub['rn']), vmax=max(sub['rn']))
    sub.plot(ax=ax, column = 'rn', norm=norm)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## National woody Features Results
# MAGIC
# MAGIC Results from the woody features based identification of relict hedges. 

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC ls "/dbfs/mnt/lab/unrestricted/elm/elm_se/testEng"

# COMMAND ----------

sf_output_relict_lengths = "/dbfs/mnt/lab/unrestricted/elm/elm_se/relict_hedge/relict_length_totals.csv"
sf_efa_hedge = 'dbfs:/mnt/lab/unrestricted/elm_data/rpa/efa_control/2023_02_07.parquet'

sdf_hedge = (spark.read.parquet(sf_efa_hedge)
             .select(
                "REF_PARCEL_SHEET_ID", 
                "REF_PARCEL_PARCEL_ID",
                F.expr("LEFT(REF_PARCEL_SHEET_ID, 2) as major_grid"),
                'LENGTH',
                io.load_geometry('wkb_geometry').alias("geometry"),
              )
)

# Load data
subdir = "relict_hedge"
datasets = ['parcels','hedgerows','nfi', 'other_woody_vom_td','woody_boundaries', 'other_woody_segments', 'available_segments', 'relict_segments']
sdf_parcels, sdf_hr_out, sdf_nfi, sdf_other_woody_vom, sdf_wb, sdf_other_woody_segments, sdf_available_segments, sdf_relict_segments = load_data(subdir = subdir, datasets = datasets)

# COMMAND ----------

# MAGIC %md
# MAGIC ### National length counts

# COMMAND ----------

sdf_parcels.count(), sdf_hedge.count(), sdf_nfi.count(), sdf_other_woody_tow.count(), sdf_other_woody_vom.count()

# COMMAND ----------

# Group by major grid and get total length, convert to pandas for plotting
df_hr_totals = (sdf_hedge
                .groupBy("major_grid")
                .agg(F.expr("SUM(length) as hedge_length"))
                .toPandas()
)

df_relict_70 = (sdf_relict_segments
                .filter(F.expr("ST_Length(geometry)>2"))
                .filter( (F.col("prop_all")>0.7) )
                .groupBy("major_grid")
                .agg(F.expr("SUM(segment_length) as relict_length_70"))
                .toPandas()
)

df_relict_30 = (sdf_relict_segments
                .filter(F.expr("ST_Length(geometry)>2"))
                .filter( (F.col("prop_all")>0.3) )
                .groupBy("major_grid")
                .agg(F.expr("SUM(segment_length) as relict_length_30"))
                .toPandas()
)

df_relict_all = (sdf_relict_segments
                .groupBy("major_grid")
                .agg(F.expr("SUM(segment_length) as relict_length_30"))
                .toPandas()
)

df_wood_features = (sdf_wb
            .groupBy("major_grid")
            .agg(F.expr("SUM(ST_Length(geometry_boundary_orig)) as orig_length"),
                 F.expr("SUM(ST_Length(geometry_hedgerow_boundary)) as boundary_hedge_length"),
                 F.expr("SUM(ST_Length(geometry_woodland_boundary)) as boundary_wood_length"),
                 F.expr("SUM(ST_Length(geometry_relict_boundary)) as boundary_relict_length"),
                 F.expr("SUM(ST_Length(geometry_boundary_relict_available)) as relict_available_length"))
            .toPandas()
)

df_boundary_length = (sdf_parcels
                     .withColumn("major_grid", F.expr("LEFT(bng_10km, 2)"))
                     .groupBy("major_grid")
                     .agg(F.expr("SUM(ST_Length(geometry_boundary)) as boundary_length"))
                     .toPandas()
)

df_avail_seg_length = (sdf_available_segments
                       .groupBy("major_grid")
                       .agg(F.expr("SUM(ST_Length(ST_GeomFromWKB(geometry))) as available_segments_length"))
                       .toPandas())

# COMMAND ----------

df_relict_totals = pd.merge(df_relict_70, df_relict_30, on = 'major_grid', how = 'outer')
df_relict_totals = pd.merge(df_relict_totals, df_wood_features, on = 'major_grid', how = 'outer')
df_relict_totals = pd.merge(df_relict_totals, df_hr_totals, on = 'major_grid', how = 'outer')
df_relict_totals = pd.merge(df_relict_totals, df_boundary_length, on = 'major_grid', how = 'outer')
df_relict_totals.head()

# COMMAND ----------

# Save the data
df_relict_totals.to_csv(sf_output_relict_lengths, index=False)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Load data and make figures

# COMMAND ----------

# Load the data
df_relict_totals = pd.read_csv(sf_output_relict_lengths)

# COMMAND ----------

# Combine dataframes and make plot comparing lengths
#df_comb = pd.merge(df_hr_totals, df_relict_totals, on='major_grid', how = 'outer')
df_relict_totals.sort_values(by='hedge_length', ascending=True, inplace=True)

f, ax = plt.subplots(figsize=(20,15))

w = 0.4
x = np.arange(df_relict_totals.shape[0])

# plot hr bars
ax.bar(x-w/2, df_relict_totals['hedge_length'].fillna(0), label='Hedgerow', width=w, color="#00a33b")
ax.bar(x+w/2, df_relict_totals['relict_length_30'].fillna(0), label='Relict Hedgerow', width=w, color="#00a33b", alpha=0.8)

ax.set_xticks(x)
ax.set_xticklabels(df_relict_totals['major_grid'], fontsize=15)
#ax.yaxis.set_ticklabel_params(size=18)
ax.tick_params(axis='y', labelsize=15)

ax.set_xlabel("Major OS Grid", fontsize=22)
ax.set_ylabel("Length", fontsize=22)
ax.legend(fontsize=18)

ax.set_title("Mapped and relict hedgerow length by OS grid, England", fontsize=22, loc='left', y = 1.05)

f.supxlabel(
    f"""
    Source: Environment Agency Vegitation Object Model $1m^2$, National Forest Inventory, Rural Payments Agency EFA Hedges
    Definitions: Relict classification based on 30% of boundary segment being intersected by tree crowns
    """,
    x=0.09,
    y=0.0,
    ha="left",
    fontsize="small",
    wrap=True,
)


# COMMAND ----------

df_eng_totals = df_relict_totals.sum(axis=0)
df_eng_totals = df_eng_totals.drop("major_grid").sort_values(ascending=False)
df_eng_totals

# COMMAND ----------

f, ax = plt.subplots(figsize=(10,10))

names = {"relict_length_30":"Relict hedgerow\n(30% threshold)", "relict_length_70":"Relict hedgerow\n(70% threshold)", "hedge_length":"Hedgerow"}
data = df_eng_totals[list(names.keys())].sort_values()
w = 0.9
x = np.arange(data.shape[0])

# plot hr bars
ax.bar(x, data, width=w, color="#00a33b")
for i, v in enumerate(data.values):
    plt.text(
        i, v+3e6,
        f"{int(np.round(v/1e6)):,}",
        fontdict={"fontsize":22, "color":"#333333", "fontweight":"normal"},
        ha="center"
    )

ax.set_xticks(x)
ax.set_xticklabels([names[i] for i in data.index])
#ax.yaxis.set_ticklabel_params(size=18)
ax.tick_params(axis='y', labelsize=15)

ax.set_xlabel("", fontsize=22)
ax.set_ylabel("Length", fontsize=22)

ax.yaxis.set_visible(False)
ax.xaxis.grid(False)

ax.set_title("Mapped and relict hedgerow length, England", fontsize=22, loc='left', y = 1.05)

f.supxlabel(
    f"""
    Units: million metres
    Source: Environment Agency Vegitation Object Model $1m^2$
    National Forest Inventory, Rural Payments Agency EFA Hedges
    """,
    x=0.090,
    y=-0.08,
    ha="left",
    fontsize="small",
    wrap=True,
)

# COMMAND ----------

# Make a stacked bar chart
import seaborn as sns
from matplotlib.ticker import FuncFormatter, PercentFormatter

def stacked_bar_parcel_counts(
    data: pd.Series, title: str, names: list
):
    
    sns.set_palette("husl")
    sns.set_style("whitegrid")
    sns.set_context("talk")

    # Initialize the matplotlib figure
    f, ax = plt.subplots(figsize=(20, 6))

    data = data[list(names.keys())]

    # Plot the total crashes
    norm = mpl.colors.Normalize(-1, len(data)+1)
    for i, (label, value) in enumerate(data.sort_values(ascending=False).items()):
        c_val = norm(i)
        p_val = value / data.sort_values(ascending=False).iloc[0]
        sns.barplot(x=[p_val], y=[''],label=names[label], color="#00a33b", alpha=c_val)
        
        #y_pos = (((i%2)*2)-1) * 0.05
        y_pos = 0.05
        str_value = str(int(np.round(value/1e6)))
        plt.text(
            p_val-0.015*len(str_value),y_pos,
            f"{int(np.round(value/1e6)):,}",
            fontdict={"fontsize":22, "color":"#333333", "fontweight":"normal"}
        )

    # Add a legend and informative axis label
    ax.legend(ncol=2, loc="upper right", frameon=True)
    ax.set(ylabel="",xlabel="")
    sns.despine(left=True, bottom=True)
    #ax.grid(False)

    f.suptitle(
        title,
        x=0.09,
        y=0.95,
        ha="left",
        fontsize=24,
    )
    f.supxlabel(
        f"""
        Units: million metres
        Source: Environment Agency Vegitation Object Model $1m^2$, National Forest Inventory, Rural Payments Agency EFA Hedges
        Definitions: Wooded classification based on length of parcel boundaries intersected by NFI Woodland. 
        Relict classification based on 30% of boundary segment being intersected by tree crowns.""",
        x=0.09,
        y=-0.2,
        ha="left",
        fontsize="small",
        wrap=True,
    )

    x_ticks = [0, 0.2, 0.4, 0.6, 0.8, 1.0]
    ax.set_xticks(x_ticks)
    ax.xaxis.set_major_formatter(PercentFormatter(xmax=1))

    return f, ax

colors = ["#484030", "#00a33b", "#A33B00", "#49A57A", "#6DB794"]
names = {"boundary_length":"Boundary length", "boundary_hedge_length":"Hedgerow", "boundary_wood_length":"Wooded boundary", "boundary_relict_length":"Relict hedge"}
f, ax = stacked_bar_parcel_counts(df_eng_totals, title="Parcel boundary lengths for England", names=names )
f.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Visualise method

# COMMAND ----------

gdfgrid1km = gpd.read_file("os_bng_grids.gpkg", layer = "1km_grid")

# COMMAND ----------

# Load data
filter_tile = "SP5833"
filter_polygon = gdfgrid1km.loc[ gdfgrid1km['tile_name']==filter_tile, "geometry"].values[0]
subdir = f"relict_hedge/test{filter_tile}" # f"test{filter_tile}v2"
datasets = ['parcels','hedgerows','nfi', 'other_woody_vom_td','woody_boundaries', 'other_woody_segments', 'available_segments', 'relict_segments']
sdf_parcels, sdf_hr, sdf_nfi, sdf_other_woody_vom, sdf_wb, sdf_other_woody_segments, sdf_available_segments, sdf_relict_segments = load_data(subdir = subdir, datasets = datasets)

# COMMAND ----------

# Load raw input data, filtered to test area

# Other woody
sdf_tow_li = spark.read.parquet(sf_tow_lidar)
sdf_tow_sp = spark.read.parquet(sf_tow_sp)
sdf_tow = (sdf_tow_li # Combine two two datasets
              .union(
                  sdf_tow_sp.select(*list(sdf_tow_li.columns)) # sdf_tow_sp has 4 additional columns that we don't want
                  )
              .withColumn("geometry", io.load_geometry(column='geometry'))
              .filter(F.expr(f"ST_Intersects(geometry, ST_GeomFromText('{filter_polygon.wkt}'))"))
)

# Other Woody - VOM-TD
sdf_vom_td = (spark.read.parquet(sf_vom_td)
              .withColumn("geometry", io.load_geometry(column='top_point', encoding_fn='ST_GeomFromText')) # Use coordinate to join to parcels for efficiency
              .filter(F.expr(f"ST_Intersects(geometry, ST_GeomFromText('{filter_polygon.wkt}'))"))
)

# COMMAND ----------

# Create geo dataframes
gdf_parcels = io.SparkDataFrame_to_PandasDataFrame(sdf_parcels)
gdf_parcels = gpd.GeoDataFrame(gdf_parcels, geometry = gpd.GeoSeries.from_wkb(gdf_parcels['geometry'], crs=27700), crs = 27700 )

gdf_hr = io.SparkDataFrame_to_PandasDataFrame(sdf_hr)
gdf_hr = gpd.GeoDataFrame(gdf_hr, geometry = gpd.GeoSeries.from_wkb(gdf_hr['geometry_hedge'], crs=27700), crs = 27700 )

gdf_woodland = io.SparkDataFrame_to_PandasDataFrame(sdf_nfi)
gdf_woodland = gpd.GeoDataFrame(gdf_woodland, geometry = gpd.GeoSeries.from_wkb(gdf_woodland['geometry_woodland'], crs=27700), crs = 27700)

#gdf_other_woody_vom = io.SparkDataFrame_to_PandasDataFrame(sdf_other_woody_vom)
#gdf_other_woody_vom = gpd.GeoDataFrame(gdf_other_woody_vom, geometry = gpd.GeoSeries.from_wkb(gdf_other_woody_vom['geometry_vom_td'], crs=27700), crs = 27700 )

#gdf_other_woody_tow = io.SparkDataFrame_to_PandasDataFrame(sdf_other_woody_tow)
#gdf_other_woody_tow = gpd.GeoDataFrame(gdf_other_woody_tow, geometry = gpd.GeoSeries.from_wkb(gdf_other_woody_tow['geometry_tow'], crs=27700), crs = 27700 )

gdf_other_woody_segments = io.SparkDataFrame_to_PandasDataFrame(sdf_other_woody_segments)
gdf_other_woody_segments = gpd.GeoDataFrame(gdf_other_woody_segments, geometry = gpd.GeoSeries.from_wkb(gdf_other_woody_segments['geometry_segment'], crs=27700), crs = 27700 )

#gdf_tow = io.SparkDataFrame_to_PandasDataFrame(sdf_tow)
#gdf_tow = gpd.GeoDataFrame(gdf_tow, geometry = gpd.GeoSeries.from_wkb(gdf_tow['geometry'], crs=27700), crs = 27700 )

# COMMAND ----------

gdf_wb = io.SparkDataFrame_to_PandasDataFrame(sdf_wb)

# COMMAND ----------

gdf_relict_segments = io.SparkDataFrame_to_PandasDataFrame(sdf_relict_segments)
gdf_relict_segments = gpd.GeoDataFrame(gdf_relict_segments, geometry = gpd.GeoSeries.from_wkb(gdf_relict_segments['geometry'], crs=27700), crs = 27700 )

# COMMAND ----------

gdf_vom_td = io.SparkDataFrame_to_PandasDataFrame(sdf_vom_td)
gdf_vom_td = gpd.GeoDataFrame(gdf_vom_td, geometry = gpd.GeoSeries.from_wkb(gdf_vom_td['geometry'], crs=27700), crs = 27700 )

# COMMAND ----------

gdf_available_segments = io.SparkDataFrame_to_PandasDataFrame(sdf_available_segments)
gdf_available_segments = gpd.GeoDataFrame(gdf_available_segments, geometry = gpd.GeoSeries.from_wkb(gdf_available_segments['geometry'], crs=27700), crs = 27700 )

# COMMAND ----------

lims = filter_polygon.bounds

# COMMAND ----------

# DBTITLE 1,Plot parcels with basemap
layers = [
    (gdf_parcels.set_geometry(gdf_parcels['geometry'].boundary), "Parcels", "black", dict(linewidth = 2)),
]

layers_plot(*layers, lims = lims, figsize=(15,15), suptitle = f"Woody Features - {filter_tile}", axtitle = "Parcels", basemap=True)

# COMMAND ----------

# DBTITLE 1,Plot raw woody features
# Define the layers I want to plot

layers = [
    (gdf_parcels.set_geometry(gdf_parcels['geometry'].boundary), "Parcels", "black", dict(linestyle=":", linewidth = 2)),
    (gdf_hr, "Hedgerow", "limegreen", dict(linewidth=3)),
    (gdf_woodland, "Woodland", "grey", dict()),
    #(gdf_tow, "TOW Tree Detections", "red", dict()),
    (gdf_vom_td.set_geometry(gdf_vom_td['crown_poly_raster'].map(lambda x: from_wkt(x))), "VOM Tree Detections", "orange", dict()),
]

layers_plot(*layers, lims = lims, figsize=(15,15), suptitle = f"Woody Features - {filter_tile}", axtitle = "Hedgerows, Woods and Trees")

# COMMAND ----------

# DBTITLE 1,Compare available boundary to segmentise available boundary
layers = [
    (gdf_wb.set_geometry(gdf_wb['geometry_boundary_relict_available'].map(lambda x: from_wkb(x))), "Available segment", "red", dict(linewidth = 2)),
    (gdf_available_segments, "Available boundary", "blue", dict(linewidth = 2)),
]

layers_plot(*layers, lims = None, figsize=(15,15), suptitle = f"Woody Features - {filter_tile}", axtitle = "Hedgerows, Woods and Trees")

# COMMAND ----------

# Plot the wood boundary after hedge and woodland

sdf_available_segments
layers = [
    (gdf_wb.set_geometry(gdf_wb['geometry_boundary_orig'].map(lambda x: from_wkb(x))), "Boundary", "black", dict(linestyle=":", linewidth = 2)),
    (gdf_wb.set_geometry(gdf_wb['geometry_hedgerow_boundary'].map(lambda x: from_wkb(x))), "Hedgerow boundary", "limegreen", dict(linewidth = 3)),
    (gdf_wb.set_geometry(gdf_wb['geometry_woodland_boundary'].map(lambda x: from_wkb(x))), "Woodland boundary", "grey", dict(linewidth = 3)),
    (gdf_wb.set_geometry(gdf_wb['geometry_boundary'].map(lambda x: from_wkb(x))), "Available boundary", "red", dict(linewidth = 3)),
]

layers_plot(*layers, lims = lims, figsize=(15,15), suptitle = f"Woody Features - {filter_tile}", axtitle = "Other woody segments", stagger=True)

# COMMAND ----------

# Plot the boundary available to be relict
layers = [
    (gdf_other_woody_segments.set_geometry(gdf_other_woody_segments['geometry_segment_orig'].map(lambda x: from_wkb(x))), "Available segments", "black", dict(linestyle=":", linewidth = 2)),
    (gdf_vom_td.set_geometry(gdf_vom_td['crown_poly_raster'].map(lambda x: from_wkt(x))), "VOM Tree Detections", "orange", dict()),
    (gdf_other_woody_segments.set_geometry(gdf_other_woody_segments['geometry_other_woody_vom_boundary'].map(lambda x: from_wkb(x))), "VOM segment portions", "red", dict(linewidth = 3)),
]

layers_plot(*layers, lims = lims, figsize=(15,15), suptitle = f"Woody Features - {filter_tile}", axtitle = "Other woody segments", stagger=False)

# COMMAND ----------

# Plot the boundary available to be relict
layers = [
    (gdf_parcels.set_geometry(gdf_parcels['geometry'].boundary), "Parcels", "black", dict(linestyle=":", linewidth = 2)),
    (gdf_wb.set_geometry(gdf_wb['geometry_hedgerow_boundary'].map(lambda x: from_wkb(x))), "Hedgerows", "limegreen", dict(linewidth=3)),
    (gdf_wb.set_geometry(gdf_wb['geometry_woodland_boundary'].map(lambda x: from_wkb(x))), "Wood", "blue", dict(linewidth=3)),
    (gdf_other_woody_segments.set_geometry(gdf_other_woody_segments['geometry_other_woody_vom_boundary'].map(lambda x: from_wkb(x))), "VOM segment portions", "orange", dict(linewidth = 3)),
]

layers_plot(*layers, lims = lims, figsize=(15,15), suptitle = f"Woody Features - {filter_tile}", axtitle = "Parcel boundaries intersected with woody features", stagger=False)

# COMMAND ----------

# Plot the relict geometries at different thresholds

layers = [
    (gdf_parcels.set_geometry(gdf_parcels['geometry'].boundary), "Parcels", "black", dict(linestyle=":", linewidth = 2)),
    #(gdf_wb.set_geometry(gdf_wb['geometry_hedgerow_boundary'].map(lambda x: from_wkb(x))), "Hedgerows", "limegreen", dict(linewidth=3)),
    #(gdf_wb.set_geometry(gdf_wb['geometry_woodland_boundary'].map(lambda x: from_wkb(x))), "Wood", "blue", dict(linewidth=3)),
    (gdf_relict_segments.loc[ gdf_relict_segments['prop_all']>0.7], "Relict hedge", "red", dict(linewidth=3)),
]

layers_plot(*layers, lims = lims, figsize=(15,15), suptitle = f"Woody Features - {filter_tile}", axtitle = "Relict hedges - 70% threshold", stagger=False)

# COMMAND ----------

layers = [
    (gdf_parcels.set_geometry(gdf_parcels['geometry'].boundary), "Parcels", "black", dict(linestyle=":", linewidth = 2)),
    #(gdf_wb.set_geometry(gdf_wb['geometry_hedgerow_boundary'].map(lambda x: from_wkb(x))), "Hedgerows", "limegreen", dict(linewidth=3)),
    #(gdf_wb.set_geometry(gdf_wb['geometry_woodland_boundary'].map(lambda x: from_wkb(x))), "Wood", "blue", dict(linewidth=3)),
    (gdf_relict_segments.loc[ gdf_relict_segments['prop_all']>0.3 ], "Relict hedge", "red", dict(linewidth=3)),
]

layers_plot(*layers, lims = lims, figsize=(15,15), suptitle = f"Woody Features - {filter_tile}", axtitle = "Relict hedges - 30% threshold", stagger=False)

# COMMAND ----------

layers = [
    (gdf_wb.set_geometry(gdf_wb['geometry_boundary_orig'].map(lambda x: from_wkb(x))), "Parcels", "black", dict(linestyle=":", linewidth = 2)),
    (gdf_wb.set_geometry(gdf_wb['geometry_hedgerow_boundary'].map(lambda x: from_wkb(x))), "Hedgerows", "limegreen", dict(linewidth=3)),
    (gdf_wb.set_geometry(gdf_wb['geometry_woodland_boundary'].map(lambda x: from_wkb(x))), "Wood", "blue", dict(linewidth=3)),
    (gdf_wb.set_geometry(gdf_wb['geometry_relict_boundary'].map(lambda x: from_wkb(x))), "Relict hedge (30% threshold)", "red", dict(linewidth=3)),
    
]

layers_plot(*layers, lims = lims, figsize=(15,15), suptitle = f"Woody Features - {filter_tile}", axtitle = "Classified parcel boundaries", stagger=False)

# COMMAND ----------


