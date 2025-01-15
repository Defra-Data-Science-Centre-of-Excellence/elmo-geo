# Databricks notebook source
# MAGIC %pip install earthaccess calplot

# COMMAND ----------



# COMMAND ----------

from elmo_geo import register
from elmo_geo.datasets.soil_moisture import soil_moisture_smap_s1_granules

register()

soil_moisture_smap_s1_granules.refresh()
df = soil_moisture_smap_s1_granules.sdf()
df.show()


# COMMAND ----------

len(df.toPandas())

# COMMAND ----------

from dotenv import load_dotenv
import earthaccess
from elmo_geo.utils.ssl import no_ssl_verification

auth = earthaccess.Auth()
load_dotenv()  # get EARTHDATA_USERNAME and EARTHDATA_PASSWORD from .env file
auth.login(strategy="environment")
if not auth.authenticated:
    raise AssertionError("Authentication failed")

with no_ssl_verification():
    store = earthaccess.Store(auth)
    store.get(granules, "/dbfs/mnt/lab/unrestricted/nasa/smap")

# COMMAND ----------

# TODO: Partition by date, not geom...
# TODO: dictionary encoding for geoms?
# Do we need to save this big joined thing? Could just calculate each time... Save the outputs with no geom - just parcel ID, granule and value...
# then repartition by geom before sending to workers

# COMMAND ----------

soil_moisture_granules_parcels_mapping.destroy()

# COMMAND ----------

soil_moisture_granules_parcels_mapping.gdf()

# COMMAND ----------

df_granule_geoms = df_granules.select("geom_hash","geometry").dropDuplicates(subset=["geom_hash","geometry"])
df_granule_geoms
print((df_granule_geoms.count(), len(df_granule_geoms.columns)))


# COMMAND ----------

from elmo_geo.datasets.rpa_reference_parcels import reference_parcels
df_parcels = reference_parcels.sdf().limit(10)
df_parcels

# COMMAND ----------

from elmo_geo.etl.transformations import sjoin_parcels

df_granules = soil_moisture_smap_s1_granules.sdf()

df_granule_geoms = df_granules.select("geom_hash","geometry").dropDuplicates(subset=["geom_hash","geometry"])

df = sjoin_parcels(df_parcels, df_granule_geoms, columns=["geom_hash"]).drop("geometry_left").withColumnRenamed("geometry_right", "geometry")
df = df.join(df_granules.select("geom_hash", "granule", "ease_grid_centre", "smap_time","s1_time"), on="geom_hash", how="left")
df.show()

# COMMAND ----------

soil_moisture_smap_s1_granules.destroy()

# COMMAND ----------

len(smap_s1_granules.file_matches) > 0

# COMMAND ----------

len("20150405T072728")

# COMMAND ----------

polygon

# COMMAND ----------



# COMMAND ----------

import shapely
england.to_crs(4326).geometry.convex_hull.map(lambda geom: shapely.geometry.polygon.orient(geom))




# COMMAND ----------

df.ease_grid_centre.sort_values().unique()

# COMMAND ----------

df.drop_duplicates(subset="geometry").explore()

# COMMAND ----------

granules = [
    "003W55N",
    "003W54N",
    "003W53N",
    "004W52N",
    "004W51N",
    "004W50N",
    "004W49N",
    "003W49N",
    "001W55N",
    "001W54N",
    "001W53N",
    "001W52N",
    "000E55N",
    "000E54N",
    "000E53N",
    "000E52N",
    "000E51N",
    "000E50N",
    "001E51N",
    "001E52N",
]

results = pd.DataFrame()
for granule in granules:

    with no_ssl_verification():
        granules = earthaccess.granule_query().short_name("SPL2SMAP_S").readable_granule_name(f"*{granule}*").get()
    if len(granules) == 0:
        raise AssertionError("No granules found")
    df = (
        pd.DataFrame.from_dict(granules)
        .loc[lambda df: df.umm.map(lambda x: "RelatedUrls" in x)]
        .assign(
            url_download=lambda df: df.umm.map(lambda x: next(url for url in x["RelatedUrls"] if url["Type"] == "GET DATA")["URL"]),
            geometry=lambda df: gpd.GeoSeries(df.umm.map(to_box), crs=4326),
            granule=lambda df: df.umm.map(lambda x: x["DataGranule"]["Identifiers"][0]["Identifier"]),
            ease_grid_centre=lambda df: df.granule.str.split("_").map(lambda x: x[7]),
            smap_time=lambda df: df.granule.str.split("_").map(lambda x: pd.Timestamp(x[5])),
            s1_time=lambda df: df.granule.str.split("_").map(lambda x: pd.Timestamp(x[6])),
            version=lambda df: df.granule.str.split("_").map(lambda x: x[-2]),
            satmodepol=lambda df: df.granule.str.split("_").map(lambda x: x[4]),
            )
        .pipe(lambda df: gpd.GeoDataFrame(df, geometry="geometry"))
        # .drop(columns=["umm", "meta"])
    )
    results = pd.concat([results, df])
results

# COMMAND ----------

from elmo_geo.datasets.ons import country_raw 
england = country_raw.gdf().loc[lambda df: df.name=="England", ["geometry"]]
england

# COMMAND ----------

england.crs

# COMMAND ----------



# COMMAND ----------

england.explore()

# COMMAND ----------

polygon = england.geometry.iloc[0].convex_hull.exterior
points = [(shapely.get_x(shapely.get_point(polygon, i)), shapely.get_y(shapely.get_point(polygon, i))) for i in range(shapely.get_num_points(polygon))]
earthaccess.granule_query().short_name("SPL2SMAP_S").polygon(points).get()

# COMMAND ----------

import shapely
shapely.get_num_points(england.geometry.iloc[0].convex_hull.exterior)

# COMMAND ----------

polygon = england.geometry.iloc[0].convex_hull.exterior
[(shapely.get_x(i), shapely.get_y(i)) for point in polygon]

# COMMAND ----------

polygon = england.geometry.iloc[0].convex_hull.exterior
[(shapely.get_x(shapely.get_point(polygon, i)), shapely.get_y(shapely.get_point(polygon, i))) for i in range(shapely.get_num_points(polygon))]

# COMMAND ----------

results.drop_duplicates(subset="geometry").explore

# COMMAND ----------

gpd.GeoSeries(gpd.GeoDataFrame(geometry=results.to_crs(27700).centroid.unique(), crs=27700).dissolve().convex_hull, crs=27700).explore()

# COMMAND ----------

gpd.GeoSeries(results.union_all(), crs=results.crs).explore()

# COMMAND ----------



# COMMAND ----------

with no_ssl_verification():
    store = earthaccess.Store(auth)
    store.get(granules, "/dbfs/mnt/lab/unrestricted/nasa/smap")

# COMMAND ----------

# MAGIC %sh ls /dbfs/mnt/lab/unrestricted/nasa/smap | wc -l

# COMMAND ----------

df.loc[df.smap_time > pd.Timestamp(year=2021, month=11,day=1)].geometry.value_counts() # geometry is consistant post Nov/Dec 2021 when Sentienl 1B went down

# COMMAND ----------

df.drop_duplicates(subset="geometry").explore()

# COMMAND ----------

# MAGIC %sh ls /dbfs/mnt/lab/unrestricted/nasa/smap

# COMMAND ----------

# plot a timeseries

# COMMAND ----------

# MAGIC %sh du /dbfs/mnt/lab/unrestricted/nasa/smap -h

# COMMAND ----------

# SMAP_L2_SM_SP_satmodepol_yyyymmddThhmmss_yyyymmddThhmmss_centre_RLVvvv_NNN.[ext
# sat is 1A or 1B Sentinel sat
# mode is IW Interferometric Wide-swath
# pol is the polarization mode (DV: Dual-polarization VV and VH)
# first date is first SMAP, second date is first Sentinel
# centre - scene centre location Approximate longitude (E or W) and latitude (N or S) of the center of the EASE-Grid area containing the Sentinel-1 radar scene. Note: This is useful for finding data over regional subsets.
# RLVvvv Composite Release ID, where: R Release L Launch Indicator (1: post-launch standard data) V 1-Digit CRID Major Version Number (Note: the data set's major version does not necessarily  coincide with the CRID major version) vvv 3-Digit CRID Minor Version Number
#https://nsidc.org/sites/default/files/spl2smap_s-v003-userguide_0.pdf
# NNN Number of times the file was generated under the same version for a particular date/time interval (002: 2nd time)
# File extensions include: .h5 HDF5 data file.xml XML Metadata fill
# descending passes are better as morning temps are more in sync

# COMMAND ----------

df.loc[df.granule == "SMAP_L2_SM_SP_1AIWDV_20241115T071159_20241116T062328_002W51N_R19240_003.h5"]

# COMMAND ----------

import os
import calplot
files = os.listdir("/dbfs/mnt/lab/unrestricted/nasa/smap")
dates = pd.Series(1, index=pd.Index([pd.Timestamp(f.split("_")[5]) for f in files]))
events = dates.resample("D").count()

calplot.calplot(events, textformat='{:.0f}', edgecolor=None, cmap='YlGn', font="")

# COMMAND ----------

import rioxarray as rxr
import xarray as xa

CRS = 27700 # 27700

# this opens the whole file, not sure how to only open the first (1km) dataset...
path = "/dbfs/mnt/lab/unrestricted/nasa/smap/SMAP_L2_SM_SP_1AIWDV_20241115T071159_20241116T062328_002W51N_R19240_003.h5"
ra = rxr.open_rasterio(path, group="Soil_Moisture_Retrieval_Data_1km", masked=True)
# need to apply the coordinates from other variables and set the CRS
ra = ra.rename_vars({var: var.replace("Soil_Moisture_Retrieval_Data_1km_", "") for var in ra.variables})
long = ra["longitude_1km"].sel(band=1, y=0.5).to_numpy()
lat = ra["latitude_1km"].sel(band=1, x=0.5).to_numpy()
ra = ra["soil_moisture_1km"].sel(band=1)
ra["x"] = long
ra["y"] = lat
ra = ra.drop_vars(["band", "spatial_ref"]) # what does spatial_ref do? by removing it here the plot is no longer squished...
ra = ra.rio.write_crs(4326).rio.reproject(CRS)
ra = ra.rename(pd.Timestamp(path.split("_")[5]))

import matplotlib.pyplot as plt
import contextily as cx

fig, ax = plt.subplots(figsize=(15,15), constrained_layout=True)
ra.plot.imshow(ax=ax, alpha=0.7, cbar_kwargs=dict(shrink=0.3))
cx.add_basemap(ax, crs=CRS, zorder=-999, source=cx.providers.CartoDB.Positron, attribution="")
ax.set_aspect("equal", "box")
ax.set_axis_off()
ax.set_title(ra.name.strftime("%A %d %B %Y, %H:%M %p"))

# COMMAND ----------

ra.plot.hist()

# COMMAND ----------

import os
granules = pd.DataFrame(dict(file=os.listdir("/dbfs/mnt/lab/unrestricted/nasa/smap")))
granules["smap_date"] = granules.file.map(lambda x: pd.Timestamp(x.split("_")[5]))
granules = granules.loc[granules.smap_date >= pd.Timestamp(year=2023, month=1, day=1)]
granules

# COMMAND ----------

import os
from pathlib import Path

import rioxarray as rxr
import xarray as xr

CRS = 27700 # 27700

# this opens the whole file, not sure how to only open the first (1km) dataset...
PATH = Path("/dbfs/mnt/lab/unrestricted/nasa/smap/")

def clean_ra(filename: str) -> xr.DataArray:
    ra = rxr.open_rasterio(PATH / filename, group="Soil_Moisture_Retrieval_Data_1km", masked=True)
    # need to apply the coordinates from other variables and set the CRS
    ra = ra.rename_vars({var: var.replace("Soil_Moisture_Retrieval_Data_1km_", "") for var in ra.variables})
    long = ra["longitude_1km"].sel(band=1, y=0.5).to_numpy()
    lat = ra["latitude_1km"].sel(band=1, x=0.5).to_numpy()
    ra = ra["soil_moisture_1km"].sel(band=1)
    ra["x"] = long
    ra["y"] = lat
    ra = ra.drop_vars(["band", "spatial_ref"]) # what does spatial_ref do? by removing it here the plot is no longer squished...
    ra = ra.rio.write_crs(4326).rio.reproject(CRS)
    ra = ra.rename(pd.Timestamp(filename.split("_")[5]))
    return ra

das = [clean_ra(filename) for filename in granules.file]
ds = xr.concat(das, dim=pd.Index([da.name for da in das], name="time")).rename("Soil moisture cm³/cm³")
ds


# COMMAND ----------

import numpy as np

print(np.half(0.001))

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(context="notebook", style="whitegrid", palette="Dark2")

fig, ax = plt.subplots(figsize=(12,6), constrained_layout=True)
ds.mean(dim=["x", "y"]).plot(ax=ax, marker="o", mec="w")
ax.set_title("")
ax.set_title("Mean soil moisture for West Midalands and Wales across 1km SMAP/Sentinel 1 product", loc="left")
ax.set_frame_on(False)
ax.set_xlabel("Date")
ax.margins(0.01)

# COMMAND ----------

pd.Index(

# COMMAND ----------


import matplotlib.pyplot as plt
import contextily as cx

fig, ax = plt.subplots(figsize=(10,10))
a[0]["Soil_Moisture_Retrieval_Data_1km_soil_moisture_1km"].sel(band=1).plot.imshow(ax=ax, alpha=0.3)
cx.add_basemap(ax, crs=4326) # 6933 4326 

# COMMAND ----------


import matplotlib.pyplot as plt
import contextily as cx

fig, ax = plt.subplots(figsize=(10,10))
df.loc[df.granule == "SMAP_L2_SM_SP_1AIWDV_20241115T071159_20241116T062328_002W51N_R19240_003.h5"].plot(alpha=0.5, ax=ax, edgecolor="r", facecolor="none", lw=2)
a[0]["Soil_Moisture_Retrieval_Data_1km_soil_moisture_1km"].sel(band=1).rio.write_crs(6933).rio.reproject(4326).plot.imshow(ax=ax, alpha=0.3)
cx.add_basemap(ax, crs=4326) # 6933 4326

# COMMAND ----------

fig, ax = plt.subplots(figsize=(10,10))
df.loc[df.granule == "SMAP_L2_SM_SP_1AIWDV_20241115T071159_20241116T062328_002W51N_R19240_003.h5"].plot(alpha=0.5, ax=ax, edgecolor="r", facecolor="none", lw=2)
cx.add_basemap(ax, crs=4326) # 6933 4326

# COMMAND ----------

# MAGIC %sh wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --keep-session-cookies --no-check-certificate --auth-no-challenge=on --http-user=burrowsej --http-password="7Fec@tcBs2r6" -r --reject "index.html*" -np -e robots=off https://n5eil01u.ecs.nsidc.org/SMAP/SPL2SMAP_S.003/2024.08.24/SMAP_L2_SM_SP_1AIWDV_20240824T002743_20240823T235036_094E44N_R19240_001.h5
# MAGIC
# MAGIC
# MAGIC # https://earthaccess.readthedocs.io/en/stable/tutorials/SSL/
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sh wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --keep-session-cookies --no-check-certificate --auth-no-challenge=on --http-user=burrowsej --http-password="7Fec@tcBs2r6" -r --reject "index.html*" -np -e robots=off https://n5eil02u.ecs.nsidc.org/egi/request?short_name=SPL2SMAP_S.003
# MAGIC

# COMMAND ----------



# COMMAND ----------

import earthaccess

USERNAME = "burrowsej"
PSWD = "7Fec@tcBs2r6"

earthaccess.login(USERNAME, PSWD)

URL = "https://n5eil01u.ecs.nsidc.org/SMAP/SPL2SMAP_S.003/2024.08.24/SMAP_L2_SM_SP_1AIWDV_20240824T002743_20240823T235036_094E44N_R19240_001.h5"
session = earthaccess.get_requests_https_session()
r = session.get(URL)
r

# COMMAND ----------

earthaccess.login(USERNAME, PSWD)
earthaccess.download(URL, ".")

# COMMAND ----------


