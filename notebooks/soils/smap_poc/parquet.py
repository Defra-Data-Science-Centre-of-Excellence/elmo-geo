# Databricks notebook source
# MAGIC %pip install earthaccess

# COMMAND ----------

from elmo_geo.io import download_link, load_sdf, ogr_to_geoparquet, read_file, to_gdf, write_parquet

from elmo_geo import register
from elmo_geo.datasets.soil_moisture import soil_moisture_smap_s1_granules, soil_moisture_geoms_parcels_mapping, GranulesModel, GeomsParcelsMappingModel
from elmo_geo.datasets.rpa_reference_parcels import reference_parcels, ReferenceParcels

register()

# COMMAND ----------

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
import numpy as np
import rioxarray as rxr
import xarray as xr

from elmo_geo.io import load_sdf
from elmo_geo.io.file import auto_repartition
from elmo_geo.utils.log import LOG

CRS = 27700

PATH = Path("/dbfs/mnt/lab/unrestricted/nasa/smap/")

# this opens the whole file, not sure how to only open the first (1km) dataset...

MYPATH = Path("/dbfs/mnt/lab/unrestricted/ELM-Project/silver/nsidc/soil_moisture_parcels.parquet")

def clean_ra(filename: str) -> xr.DataArray:
    """Open a raster image, pick and rename the band, set the x and y coords and CRS.
    
    Parameters:
        filename: Name of the granule/image file.
    """
    ra = rxr.open_rasterio(PATH / filename, group="Soil_Moisture_Retrieval_Data_1km", masked=True)
    # need to apply the coordinates from other variables and set the CRS
    ra = ra.rename_vars({var: var.replace("Soil_Moisture_Retrieval_Data_1km_", "") for var in ra.variables})
    lon = ra["longitude_1km"].sel(band=1, y=0.5).to_numpy()
    lat = ra["latitude_1km"].sel(band=1, x=0.5).to_numpy()
    ra = ra["soil_moisture_1km"].sel(band=1)
    ra["x"] = lon
    ra["y"] = lat
    ra = ra.drop_vars(["band", "spatial_ref"]) # what does spatial_ref do? by removing it here the plot is no longer squished...
    ra = ra.rio.write_crs(4326).rio.reproject(CRS)
    ra = ra.rename(filename)
    return ra


def make_ds(granules: list[str]) -> xr.DataArray:
    """Clean and stack multiple images into a 3D data cube for faster indexing.

    Must have same x and y coordinates (tile geometry).

    Parameters:
        granules: A list of granule names.
    Returns:
        A 3D data array with x, y and granule dimensions.
    """
    # TODO: is this a DataArray or DataSet?
    das = [clean_ra(filename) for filename in granules]
    return xr.concat(das, dim=pd.Index([da.name for da in das], name="granule")).rename("Soil moisture cm³/cm³")


def _get_centroid_values_by_geom_hash(pdf: pd.DataFrame) -> pd.DataFrame:
    """Lookup the soil moisture value for each parcel and granule"""
    if pdf.geom_hash.nunique() > 1:
        msg = f"There are more than one geom_hash in this group: {pdf.geom_hash.unique()}"
        raise ValueError(msg)
    ds = make_ds(pdf.granule.unique())
    geoms = gpd.GeoSeries.from_wkb(pdf.wkb, crs=27700).to_crs(epsg=ds.rio.crs.to_epsg())
    # vectorised pointwise indexing by using shared dimension name "points"
    x_index = ds.x.sel(x=xr.DataArray(geoms.x, dims="points"), method="nearest")
    y_index = ds.y.sel(y=xr.DataArray(geoms.y, dims="points"), method="nearest")
    g_index = xr.DataArray(pdf.granule, dims="points")
    return pdf.copy().assign(moisture=ds.sel(x=x_index, y=y_index, granule=g_index)).drop(columns=["wkb"]) # do we need to copy here?

def func(
    soil_moisture_smap_s1_granules: GranulesModel,
    soil_moisture_geoms_parcels_mapping: GeomsParcelsMappingModel,
    reference_parcels: ReferenceParcels,
    ):
    # check if geometry/parcel intersection needs refreshing - partition this correctly
    df_granules = soil_moisture_smap_s1_granules.sdf().filter("month LIKE '2018%'").filter("ease_grid_centre NOT LIKE '000E55N'")
    df_geoms_parcels = soil_moisture_geoms_parcels_mapping.sdf()
    all_geoms = {a["geom_hash"] for a in df_granules.select("geom_hash").distinct().collect()}
    intersected_geoms = {a["geom_hash"] for a in df_geoms_parcels.select("geom_hash").distinct().collect()}
    missing_geoms = all_geoms - intersected_geoms
    if len(missing_geoms) > 0:
        df_geoms_parcels.refresh()

    # check which granules have not been run and get their months
    all_granules = {g.granule for g in df_granules.select("granule").distinct().collect()}
    processed_granules = set()
    if MYPATH.exists():
        processed_granules = {g.granule for g in load_sdf(str(MYPATH)).select("granule").distinct().collect()}
    todo_granules = all_granules - processed_granules
    if (n_granules := len(todo_granules)) > 0:
        todo_months = {m.month for m in df_granules.filter(F.col("granule").isin(todo_granules)).select("month").distinct().collect()}
        msg = (
            f"Found {n_granules:,.0f} granules to be processed\n"
            f"Processing granules:{sorted(list(todo_granules))}\n" # delete this. Issue is some granules hane no valid parcels so we keep running them...
            f"Found {len(todo_months):,.0f} months to process\n"
            f"Processing months:{sorted(list(todo_months))}"
        )
        LOG.info(msg)
    else:
        return "hi" # TODO: need a way of stoping the refresh... All of the above could be in a separate custom is fresh function... would need to pass the todo_months across though...
    
    # join datasets together and repartition
    df = (
        soil_moisture_smap_s1_granules.sdf()
        .select("granule", "geom_hash", "month", "smap_time")
        .filter("month LIKE '2018%'")
        .filter("ease_grid_centre NOT LIKE '000E55N'")
        .filter(F.col("month").isin(todo_months)) # TODO: drop 2024 filter
        .join(soil_moisture_geoms_parcels_mapping.sdf(), "geom_hash", "inner")
        .join(
            reference_parcels.sdf()
            .select("id_parcel", "geometry")
            .withColumn("wkb", F.expr("ST_AsEWKB(ST_Centroid(geometry))"))
            .drop("geometry"),
            "id_parcel",
            "inner",
            )
        # repartition to number of granules - should be about 1m rows on average?! seems high
        .transform(lambda sdf: sdf.repartition(n_granules*4, "geom_hash", "month"))
    )
    # group by tile geometry and month and distribute out to lookup values in images
    schema = T.StructType(
        [f for f in df.schema.fields if f.name != "wkb"]
        + [T.StructField("moisture", T.FloatType())]
        )
    return (
        df
        .groupby("geom_hash", "month")
        .applyInPandas(_get_centroid_values_by_geom_hash, schema=schema)
        .dropna()
        .repartition(10_000, "month", "geom_hash")
    )


# check if exists, if it does work out which granules are run and filter df_granules

# for df_granules, join in parcels geoms, run intersection...
# df = func(soil_moisture_smap_s1_granules, soil_moisture_geoms_parcels_mapping, reference_parcels)

# duplicated granules...
# drop 000E55N


# COMMAND ----------

df = (
        soil_moisture_smap_s1_granules.sdf()
        .select("granule", "geom_hash", "month", "smap_time")
        .filter("month LIKE '2024%'")
        .filter("ease_grid_centre NOT LIKE '000E55N'")
        .join(soil_moisture_geoms_parcels_mapping.sdf(), "geom_hash", "inner")
        .join(
            reference_parcels.sdf()
            .select("id_parcel", "geometry")
            .withColumn("wkb", F.expr("ST_AsEWKB(ST_Centroid(geometry))"))
            .drop("geometry"),
            "id_parcel",
            "inner",
            )
        # repartition to number of granules - should be about 1m rows on average?! seems high
        .transform(lambda sdf: sdf.repartition(1042*4, "geom_hash", "month"))
    )

# COMMAND ----------

schema = T.StructType(
        [f for f in df.schema.fields if f.name != "wkb"]
        + [T.StructField("moisture", T.FloatType())]
        )
df = (
    df
    .groupby("geom_hash", "month")
    .applyInPandas(_get_centroid_values_by_geom_hash, schema=schema)
    .dropna()
)
df

# COMMAND ----------

PATH_DATASET = "/dbfs/mnt/lab/unrestricted/ELM-Project/silver/nsidc/soil_moisture_parcels.parquet"
write_parquet(df, PATH_DATASET, partition_cols=["month", "geom_hash"], mode="append")

# COMMAND ----------

# MAGIC %sh du -h --max-depth=1 /dbfs/mnt/lab/unrestricted/ELM-Project/silver/nsidc/soil_moisture_parcels.parquet

# COMMAND ----------

df_granules = soil_moisture_smap_s1_granules.sdf().filter("month LIKE '2018%'").filter("ease_grid_centre NOT LIKE '000E55N'")
df_geoms_parcels = soil_moisture_geoms_parcels_mapping.sdf()
all_geoms = {a["geom_hash"] for a in df_granules.select("geom_hash").distinct().collect()}
intersected_geoms = {a["geom_hash"] for a in df_geoms_parcels.select("geom_hash").distinct().collect()}
missing_geoms = all_geoms - intersected_geoms
if len(missing_geoms) > 0:
    # df_geoms_parcels.refresh()
    print("NO")


# COMMAND ----------

all_granules = {g.granule for g in df_granules.select("granule").distinct().collect()}
processed_granules = set()
if MYPATH.exists():
    processed_granules = {g.granule for g in load_sdf(str(MYPATH)).select("granule").distinct().collect()}
todo_granules = all_granules - processed_granules

# COMMAND ----------

todo_granules

# COMMAND ----------


if (n_granules := len(todo_granules)) > 0:
    todo_months = {m.month for m in df_granules.filter(F.col("granule").isin(todo_granules)).select("month").distinct().collect()}
    msg = (
        f"Found {n_granules:,.0f} granules to be processed\n"
        f"Processing granules:{sorted(list(todo_granules))}\n" # delete this. Issue is some granules hane no valid parcels so we keep running them...
        f"Found {len(todo_months):,.0f} months to process\n"
        f"Processing months:{sorted(list(todo_months))}"
    )
    LOG.info(msg)

# COMMAND ----------

df = (
        soil_moisture_smap_s1_granules.sdf()
        .select("granule", "geom_hash", "month", "smap_time")
        .filter(F.col("month").isin(todo_months))
        .join(soil_moisture_geoms_parcels_mapping.sdf(), "geom_hash", "inner")
        .join(
            reference_parcels.sdf()
            .select("id_parcel", "geometry")
            .withColumn("wkb", F.expr("ST_AsEWKB(ST_Centroid(geometry))"))
            .drop("geometry"),
            "id_parcel",
            "inner",
            )
        # repartition to number of granules - should be about 1m rows on average?! seems high
        .transform(lambda sdf: sdf.repartition(n_granules*4, "geom_hash", "month"))
    )
df

# COMMAND ----------

df.count()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# group by tile geometry and month and distribute out to lookup values in images
schema = T.StructType(
    [f for f in df.schema.fields if f.name != "wkb"]
    + [T.StructField("moisture", T.FloatType())]
    )
df = (
    df
    .groupby("geom_hash", "month")
    .applyInPandas(_get_centroid_values_by_geom_hash, schema=schema)
    .dropna()
    .repartition(10_000, "month", "geom_hash")
)
df.display()

# COMMAND ----------

spark.read.parquet("dbfs:/mnt/lab/unrestricted/ELM-Project/silver/nsidc/soil_moisture_parcels.parquet").filter("month='2015_01'")

# COMMAND ----------

df = df.join(
    reference_parcels.sdf()
    .select("id_parcel", "geometry")
    .withColumn("wkb", F.expr("ST_AsEWKB(ST_Centroid(geometry))"))
    .drop("geometry"),
    "id_parcel",
    "inner",
    )
df

# COMMAND ----------

df = df.repartition(10_000, "geom_hash", "month")
df

# COMMAND ----------

# df = df.transform(lambda sdf: sdf.repartition(round(df.rdd.countApprox(1_000, 0.6) / 500_000), "geom_hash", "month"))
# df

# COMMAND ----------

# group by tile geometry and month and distribute out to lookup values in images
schema = T.StructType([f for f in df.schema.fields if not f.name in ("geometry","wkb","moisture")] + [T.StructField("moisture", T.FloatType())] )
schema

# COMMAND ----------

def _get_centroid_values_by_geom_hash(pdf: pd.DataFrame) -> pd.DataFrame:
    """Lookup the soil moisture value for each parcel and granule"""
    if pdf.geom_hash.nunique() > 1:
        msg = f"There are more than one geom_hash in this group: {pdf.geom_hash.unique()}"
        raise ValueError(msg)
    ds = make_ds(pdf.granule.unique())
    geoms = gpd.GeoSeries.from_wkb(pdf.wkb, crs=27700).to_crs(epsg=ds.rio.crs.to_epsg())
    # vectorised pointwise indexing by using shared dimension name "points"
    x_index = ds.x.sel(x=xr.DataArray(geoms.x, dims="points"), method="nearest")
    y_index = ds.y.sel(y=xr.DataArray(geoms.y, dims="points"), method="nearest")
    g_index = xr.DataArray(pdf.granule, dims="points")
    return pdf.copy().assign(moisture=ds.sel(x=x_index, y=y_index, granule=g_index)).drop(columns=["wkb"]) # do we need to copy here?

df = (
    df
    .groupby("geom_hash", "month")
    .applyInPandas(_get_centroid_values_by_geom_hash, schema=schema)
)
df.display()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.count()

# COMMAND ----------

.select("id_parcel", "month", "smap_time", "moisture", "granule", "geom_hash")
    .dropna()
    .transform(lambda sdf: sdf.repartition(round(df.rdd.countApprox(1_000, 0.6) / 500_000), "geom_hash", "month"))

# COMMAND ----------

df.select("geom_hash").distinct().count()

# COMMAND ----------

df.select("month").distinct().count()

# COMMAND ----------

df.rdd.groupByKey().count()

# COMMAND ----------

25*12

# COMMAND ----------

df = df.join(reference_parcels.sdf().select("id_parcel", "geometry").withColumn("wkb", F.expr("ST_AsEWKB(ST_Centroid(geometry))")).drop("geometry"), "id_parcel", "inner")
df.count()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

reference_parcels.sdf().select("id_parcel", "geometry").withColumn("wkb", F.expr("ST_AsEWKB(ST_Centroid(geometry))")).drop("geometry").count()

# COMMAND ----------

import pyspark.sql.functions as F
df = load_sdf("/dbfs/tmp/ed_moisture.parquet").filter(F.col("month").isin("2024_03", "2024_01"))
write_parquet(df, "/dbfs/tmp/ed_moisture_11.parquet", partition_cols=["month"], mode="append")

# COMMAND ----------

load_sdf("/dbfs/tmp/ed_moisture_11.parquet").count() # maxRecordsPerFile

# COMMAND ----------

# MAGIC %sh du -h /dbfs/tmp/ed_moisture_11.parquet

# COMMAND ----------

# MAGIC %sh du -h /dbfs/tmp/ed_moisture_11.parquet

# COMMAND ----------

df.repartition(2000, "month").rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %sh ls /dbfs/tmp/ed_moisture.parquet/month=2024_04

# COMMAND ----------

# MAGIC %sh du -h /dbfs/tmp/ed_moisture.parquet

# COMMAND ----------

# MAGIC %sh ls /dbfs/tmp/ed_moisture.parquet

# COMMAND ----------

# MAGIC %sh du -h /dbfs/tmp/ed_moisture.parquet

# COMMAND ----------

pdf = df.repartition("month").toPandas()
pdf




# COMMAND ----------

pdf.to_parquet("/dbfs/tmp/edtest1.parquet", partition_cols=["month"], existing_data_behavior="delete_matching")

# COMMAND ----------

from elmo_geo.io import write_parquet

# write_parquet(df, path="/dbfs/tmp/ed_moisture.parquet", partition_cols=["month"], mode="append")
df = df.repartition(2000, "month")

df.write.mode("append").parquet("dbfs:/tmp/ed_moisture.parquet", partitionBy=["month"])

# COMMAND ----------

load_sdf("/dbfs/tmp/ed_moisture.parquet").filter("id_parcel = 'SO34248014'").toPandas().set_index("smap_time").sort_index().moisture.plot(title="SO34248014")

# COMMAND ----------

df.set_index("smap_time").resample("MS").moisture.mean()

# COMMAND ----------

now need to know which granules are new i.e. haven't been processed, grab their months and rerun those appending... Can't do this easerly without saving granule...

# COMMAND ----------

df.moisture.plot.hist(bins=50)

# COMMAND ----------

123057044/301132369

# COMMAND ----------

df.dtypes

# COMMAND ----------

df["geom_hash"] = pd.Categorical(df["geom_hash"].values, set(df["geom_hash"].values))

# COMMAND ----------

df["geom_hash"].values.astype("category")

# COMMAND ----------

pd.Series(df["geom_hash"].values, dtype="category")

# COMMAND ----------

pd.Categorical(df["granule"].values, set(df["granule"].values))

# COMMAND ----------

df["geom_hash"].unique()

# COMMAND ----------

set(df["geom_hash"])

# COMMAND ----------

set(df["geom_hash"].values)

# COMMAND ----------

df["granule"] = df["granule"].astype(str)

# COMMAND ----------

# MAGIC %sh ls /dbfs/tmp/ed_moisture1.parquet/month=2024_01
# MAGIC

# COMMAND ----------

# MAGIC %sh ls /dbfs/tmp/ed_moisture3.parquet/month=2024_01
# MAGIC

# COMMAND ----------

import pandas as pd
df = pd.read_parquet("/dbfs/tmp/ed_moisture1.parquet")
df

# COMMAND ----------

df.drop(columns="geom_hash").to_parquet("/dbfs/tmp/ed_moisture3.parquet", partition_cols=["month"])

# COMMAND ----------

# MAGIC %sh ls /dbfs/tmp/ed_moisture1.parquet
# MAGIC

# COMMAND ----------

df = pd.read_parquet("/dbfs/tmp/ed_moisture3.parquet")
df

# COMMAND ----------


