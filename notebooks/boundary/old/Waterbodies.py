# Databricks notebook source
# MAGIC %pip install -qU osmnx osdatahub contextily folium

# COMMAND ----------


import contextily as cx
import geopandas as gpd
import matplotlib.pyplot as plt
import osmnx as ox
import pandas as pd
from osdatahub import Extent
from osdatahub.FeaturesAPI import FeaturesAPI
from osdatahub.filters import is_like

ox.settings.cache_folder = "/dbfs/tmp/"

key = "WxgUdETn6cy58WZkfwZ7wdMVLlt5eDsX"

crs = 27700
mask = ox.geocode_to_gdf("Northumberland").to_crs(crs)

# COMMAND ----------

water = [
    "/dbfs/mnt/base/unrestricted/source_data_gov_uk/dataset_habnet_eng_lakes/format_SHP_habnet_eng_lakes/SNAPSHOT_2022_12_08_habnet_eng_lakes/NHN_v02_LAK.shp",
    "/dbfs/mnt/base/unrestricted/source_data_gov_uk/dataset_habnet_eng_rivers/format_SHP_habnet_eng_rivers/SNAPSHOT_2022_12_08_habnet_eng_rivers/NHN_v02_RIV.shp",
    "/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_wfd_lake_water_bodies_cycle_2_classification_2019/format_SHP_wfd_lake_water_bodies_cycle_2_classification_2019/SNAPSHOT_2021_03_17_wfd_lake_water_bodies_cycle_2_classification_2019/WFD_Lake_Water_Bodies_Cycle_2_2019.shp",
    "/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_wfd_river_basin_districts_cycle_2/format_SHP_wfd_river_basin_districts_cycle_2/SNAPSHOT_2021_03_17_wfd_river_basin_districts_cycle_2/WFD_River_Basin_Districts_Cycle_2.shp",
    "/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_wfd_river_waterbody_catchments_cycle_2/format_SHP_wfd_river_waterbody_catchments_cycle_2/SNAPSHOT_2021_03_17_wfd_river_waterbody_catchments_cycle_2/WFD_River_Water_Body_Catchments_Cycle_2.shp",
    "/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_wfd_surface_water_management_catchments_cycle_2/format_SHP_wfd_surface_water_management_catchments_cycle_2/SNAPSHOT_2021_03_17_wfd_surface_water_management_catchments_cycle_2/WFD_Surface_Water_Management_Catchments_Cycle_2.shp",
    "/dbfs/mnt/base/unrestricted/source_ordnance_survey_data_hub/dataset_rivers_canals_streams_vector/format_SHP_rivers_canals_streams_vector/SNAPSHOT_2021_03_16_rivers_canals_streams_vector/WatercourseLink.shp",
]

water_names = [
    #   'govuk/habnet_eng_lakes/NHN_v02_LAK',
    #   'govuk/habnet_eng_rivers/NHN_v02_RIV',
    "defra/WFD_Lake_Water_Bodies_Cycle_2_2019",
    #   'defra/WFD_River_Basin_Districts_Cycle_2',
    #   'defra/WFD_River_Water_Body_Catchments_Cycle_2',
    #   'defra/WFD_Surface_Water_Management_Catchments_Cycle_2',
    "os/WatercourseLink",
]

gdf2 = gpd.read_file(water[2], mask=mask).to_crs(crs)
gdf6 = gpd.read_file(water[6], mask=mask).to_crs(crs)


display(gdf2)
display(gdf6)


fig, ax = plt.subplots(figsize=(9, 16))
gdf6.plot(ax=ax, color="C4", alpha=0.3)
gdf2.plot(ax=ax)
cx.add_basemap(ax=ax, crs=crs, source=cx.providers.OpenTopoMap, alpha=0.3)

# COMMAND ----------

# place, tags = 'Northumberland', {'water':True,'waterway':True}
# gdf = (ox.geometries_from_place(place, tags)
#   .reset_index()
#   .assign(gtype = lambda df: df.geom_type)
#   [['osmid', *tags.keys(), 'gtype', 'geometry']]
#   .to_crs(crs)
# )


display(gdf)


fig, ax = plt.subplots(figsize=(9, 16))
gdf[gdf["gtype"] != "Polygon"].plot(ax=ax, color="C4", alpha=0.3)
gdf[gdf["gtype"] == "Polygon"].plot(ax=ax, color="C0")
cx.add_basemap(ax=ax, crs=crs, source=cx.providers.OpenTopoMap, alpha=0.3)

# COMMAND ----------


def gdf_from_product(product):
    extent = Extent.from_bbox(mask.total_bounds, crs=f"EPSG:{crs}")
    water = is_like("Theme", "*Water*")
    features = FeaturesAPI(key, product, extent)
    features.add_filters(water)
    results = features.query(1_000_000)
    return gpd.GeoDataFrame.from_features(results)


gdf = pd.concat(
    [
        gdf_from_product("Topography_TopographicArea"),
        gdf_from_product("Topography_TopographicLine"),
    ],
)


display(gdf)

fig, ax = plt.subplots(figsize=(9, 16))
gdf[gdf.geom_type != "Polygon"].plot(ax=ax, color="C4", linewidth=1)
gdf[gdf.geom_type == "Polygon"].plot(ax=ax, color="C0")
cx.add_basemap(ax=ax, crs=crs, source=cx.providers.OpenTopoMap, alpha=0.3)

# COMMAND ----------

gdf.to_parquet("/dbfs/tmp/os_water.parquet")
