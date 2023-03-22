# MAGIC %md # Bare Soil Survey Data Cleaning
# MAGIC We have received survey data so we can evaluate
# MAGIC model analysis. We would like to see whether
# MAGIC the satellite data lines up with the survey data that we have got.
# MAGIC
# MAGIC In this notwbook, we will:
# MAGIC - Locate data required from 3 sources: Parcel geometries,
# MAGIC point of survey and NDVI calculated for point vfrom Sentinel 2 data.
# MAGIC - overlay points to see which parcels have survey points located inside
# MAGIC - Find NDVI calculated for each point

# COMMAND ----------

# %load_ext autoreload
# %autoreload 2
# %pip install -qU git+https://github.com/aw-west-defra/cdap_geo.git
# %pip install contextily

# COMMAND ----------

import os

import contextily as cx
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rioxarray as rxr
from cdap_geo import st_join
from geopandas import GeoDataFrame
from pyspark.sql import functions as F
from sedona.register import SedonaRegistrator

SedonaRegistrator.registerAll(spark)

# COMMAND ----------

# locating data
path_parcels = "/mnt/lab/unrestricted/elm/elmo/baresoil/parcels.parquet"
path_surveys = "/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/survey_data"
year = 2023
month_fm = f"{year-1}-11"
month_to = f"{year}-02"


files = [f for f in os.listdir(path_surveys) if f.startswith("SFI_") and f.endswith(".csv")]
print(f"Found {len(files)} matching files")

# COMMAND ----------

files_paths = [f"{path_surveys.replace('/dbfs','')}/{f}" for f in files]
files_iter = iter(files_paths)
col_names = [
    "Farm-FieldRLR_Number",
    "Groundcover-QuadratLocation-Latitude",
    "Groundcover-QuadratLocation-Longitude",
    "Groundcover-BareGround",
]
survey_df = spark.read.csv(next(files_iter), header=True, inferSchema=True).select(col_names)
for f in files_iter:
    survey_df = survey_df.union(
        (spark.read.csv(f, header=True, inferSchema=True).select(col_names))
    )
survey_df = (
    survey_df.withColumnRenamed("Groundcover-QuadratLocation-Latitude", "latitude")
    .withColumnRenamed("Groundcover-QuadratLocation-Longitude", "longitude")
    .filter("latitude is not null AND longitude is not null")
    .select(
        F.col("Farm-FieldRLR_Number").alias("Farm_number"),
        (F.col("Groundcover-BareGround") / 100).alias("bareground_percent_survey"),
        F.expr('ST_Transform(ST_Point(latitude, longitude), "epsg:4326","epsg:27700")').alias(
            "geometry"
        ),
    )
)

# COMMAND ----------

# getting parcels compatible for survey
parcel_df = spark.read.parquet(path_parcels)

parcel_df = parcel_df.select("id_parcel", "tile", "geometry").withColumn(
    "geometry", F.expr("ST_GeomFromWKB(hex(geometry))")
)
display(parcel_df)

# COMMAND ----------

# Combining parcel and survey data frames to check points inside each tile
df = (
    (st_join(parcel_df, survey_df))
    .withColumnRenamed("geometry_left", "polygon_geometry")
    .withColumnRenamed("geometry_right", "point_geometry")
)
df = (
    df.withColumn("polygon_geometry", F.expr("ST_AsBinary(polygon_geometry)"))
    .withColumn("point_geometry", F.expr("ST_AsBinary(point_geometry)"))
    .toPandas()
    .pipe(
        lambda pdf: gpd.GeoDataFrame(
            {
                "id_parcel": pdf["id_parcel"],
                "tile": pdf["tile"],
                "bareground_percent_survey": pdf["bareground_percent_survey"],
                "polygon_geometry": gpd.GeoSeries.from_wkb(pdf["polygon_geometry"], crs=27700),
                "point_geometry": gpd.GeoSeries.from_wkb(pdf["point_geometry"], crs=27700),
            },
            crs=27700,
            geometry="point_geometry",
        )
    )
)

# COMMAND ----------

print(
    f"There are {df['id_parcel'].nunique()} distinct parcels in the surveys",
    f" in the dataset. \n These parcels are in {df['tile'].nunique()} tiles",
    f" around Enland. \n The following tiles: {df['tile'].unique()}",
)
df.head()

# COMMAND ----------

df = df.to_crs(epsg=32630).set_geometry("point_geometry")


def get_ndvi(gdf: GeoDataFrame, path_ndvi: str) -> GeoDataFrame:
    """Lookup NDVI for points in a dataframe and return with a new `ndvi` column"""
    if any(gdf.geometry.geom_type != "Point"):
        raise ValueError("Geometries must be points to lookup NDVI")
    da = rxr.open_rasterio(path_ndvi).squeeze(drop=True)
    if gdf.crs != da.rio.crs.to_epsg():
        raise ValueError(
            f"CRS of geometries must manage that of the image. Image is {da.rio.crs.to_epsg()}, "
            f"geometries are {gdf.crs}"
        )
    return gdf.assign(
        ndvi=gdf.geometry.map(lambda p: da.sel(x=p.x, y=p.y, method="nearest").values.item())
    )


df = df.groupby("tile", group_keys=False).apply(
    lambda gdf: get_ndvi(
        gdf,
        path_ndvi=(
            f"/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/"
            f"ndvi/T{gdf.name}-{month_fm}-survey.tif"
        ),
    )
)

# COMMAND ----------

# Quick checks to see if data is there
df_na = df.loc[df["ndvi"].isna(), :]

if df_na.shape[0] > 0:
    print(
        f"There are {df_na.shape[0]} NA values in the ",
        "dataframe. This is likely due to the cloud cover from the NDVI files.",
        f" All these na values are from the following tiles: {set(df_na['tile'])}",
    )
    df_nona = df.dropna()
else:
    print("There are no ull NDVI values in the dataset.")

# COMMAND ----------

# checking the NaN values
if any(df["ndvi"].isna()):
    # visualising a na parcel one at a time (change num ffor next parcel in df_na)
    df["null_flag"] = df["ndvi"].isna()
    print(f"We found that {df_na['id_parcel'].nunique()} parcels have na results.")

    df = df.to_crs(epsg=27700).set_geometry("point_geometry")
    # findig parcel data
    num = 0
    na_parcels = list(set([p for p in df_na["id_parcel"]]))
    df_poly = (
        df.loc[df["id_parcel"] == na_parcels[num]]
        .to_crs(epsg=32630)
        .set_geometry("polygon_geometry")
        .to_crs(epsg=32630)
    )
    parcel_geom = list(set([p for p in df_poly["polygon_geometry"]]))[0]
    # finding polygon data
    path_ndvi = (
        "/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/"
        f"ndvi/T{df_poly['tile'].unique()[0]}-{month_fm}-survey.tif"
    )
    da = rxr.open_rasterio(path_ndvi).squeeze(drop=True)
    raster_clip_box = da.rio.clip_box(*parcel_geom.bounds)
    # visualisation
    fig, ax = plt.subplots(figsize=(6, 6))
    raster_clip_box.plot(ax=ax)
    df_poly["polygon_geometry"].plot(ax=ax, alpha=0.1, color="gray")
    df_poly["point_geometry"].plot(ax=ax, alpha=0.6, color="C1")
    df_poly.loc[df_poly["null_flag"], "point_geometry"].plot(ax=ax, alpha=0.7, color="C3")
    cx.add_basemap(ax, crs=32630, source=cx.providers.OpenTopoMap)
    fig.show()
else:
    print("There are no Null values requiring to be checked.")

# COMMAND ----------

if df_na.shape[0] > 0:
    pdf = pd.DataFrame(df).drop(["point_geometry", "polygon_geometry", "null_flag"], axis=1)
else:
    pdf = pd.DataFrame(df).drop(["point_geometry", "polygon_geometry"], axis=1)

# COMMAND ----------

wfm_path = "/mnt/lab/unrestricted/edward.burrows@defra.gov.uk/parcels_v2.parquet"
cols = spark.read.parquet(wfm_path).columns
cols = [col for col in cols if col in ("id_business", "id_parcel") or col.startswith("ha_")]
wfm = spark.read.parquet(wfm_path).select(*cols).repartition(100).toPandas()
pdf = pdf.set_index("id_parcel").join(wfm.set_index("id_parcel"), how="left")

# COMMAND ----------


def add_bool_col(df, col_list, new_col_name):
    """Adds a boolean column to dataframe."""
    df_filtered = df.loc[:, col_list]
    df[new_col_name] = df_filtered.sum(axis=1) > 0
    return df


def column_finder(df, criteria):
    return [c for c in df.columns if c.startswith(criteria)]


# arable soil column
predicate = column_finder(pdf, "ha_arable_")
pdf = add_bool_col(pdf, predicate, "arable")

# improved grassland column
predicate = column_finder(pdf, "ha_grassland_")
pdf = add_bool_col(pdf, predicate, "grassland")
print(pdf.shape)
pdf.head()


# COMMAND ----------

# adding flag for field type
pdf["field_type"] = np.where(pdf.arable, "Arable", np.where(pdf.grassland, "Grassland", "Arable"))
parcel_dict = {id: field for id, field in zip(pdf.index, pdf["field_type"])}
df_parcel = pd.DataFrame.from_dict(
    parcel_dict, orient="index", columns=["field_type"]
).reset_index()
df_grouped = df_parcel.groupby("field_type").count()
df_grouped

# COMMAND ----------

# cleaning before saving dataframe
pdf = pdf[["tile", "bareground_percent_survey", "ndvi", "field_type"]].reset_index()
pdf["ndvi"] = np.where(pdf["ndvi"] < 0.02, np.nan, pdf["ndvi"])
pdf.head()

# COMMAND ----------

# Saving dataframe
path_output = "/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/survey_data/survey_ndvi_dataset.csv"
pdf.to_parquet(path_output)
print(f"This dataset has been saved to {path_output}")
