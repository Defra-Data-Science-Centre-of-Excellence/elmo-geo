# Databricks notebook source
# MAGIC %md
# MAGIC # Convert Base to Parquet
# MAGIC For datasets in base resave as parquet for speed and efficiency.  
# MAGIC Geometries in parquet are in Sedona's udt format, using British National Grid (crs=epsg:27700), and precision is reduced to 1mm.
# MAGIC
# MAGIC ### Datasets
# MAGIC - rpa, land cover  
# MAGIC - rpa, reference parcels  
# MAGIC - rpa, efa hedges  
# MAGIC - os, watercourses  

# COMMAND ----------

f_rpa_ref_parcels = '/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_reference_parcels/format_GPKG_rpa_reference_parcels/SNAPSHOT_2023_02_07_rpa_reference_parcels/reference_parcels.zip/reference_parcels.gpkg'
f_parcels = '/dbfs/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_02_07.parquet'


f_rpa_efa_hedges = '/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_efa_control_layer/format_GPKG_efa_control_layer/SNAPSHOT_2023_02_07_efa_control_layer/LF_CONTROL_MV.zip/LF_CONTROL_MV.gpkg'
f_hedges = '/dbfs/mnt/lab/unrestricted/elm_data/rpa/efa_hedges/2023_02_07.parquet'


f_rpa_landcover = '/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_land_cover/format_GPKG_rpa_land_cover/SNAPSHOT_2023_02_07_rpa_land_cover/land_cover.zip/land_cover.gpkg'
f_landcover = '/dbfs/mnt/lab/unrestricted/elm_data/rpa/land_cover/2023_02_07.parquet'


# COMMAND ----------

water = [

'/dbfs/mnt/base/unrestricted/source_data_gov_uk/dataset_habnet_eng_lakes/format_SHP_habnet_eng_lakes/SNAPSHOT_2022_12_08_habnet_eng_lakes/NHN_v02_LAK.shp',

'/dbfs/mnt/base/unrestricted/source_data_gov_uk/dataset_habnet_eng_rivers/format_SHP_habnet_eng_rivers/SNAPSHOT_2022_12_08_habnet_eng_rivers/NHN_v02_RIV.shp',

'/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_wfd_lake_water_bodies_cycle_2_classification_2019/format_SHP_wfd_lake_water_bodies_cycle_2_classification_2019/SNAPSHOT_2021_03_17_wfd_lake_water_bodies_cycle_2_classification_2019/WFD_Lake_Water_Bodies_Cycle_2_2019.shp',

'/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_wfd_river_basin_districts_cycle_2/format_SHP_wfd_river_basin_districts_cycle_2/SNAPSHOT_2021_03_17_wfd_river_basin_districts_cycle_2/WFD_River_Basin_Districts_Cycle_2.shp',

'/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_wfd_river_waterbody_catchments_cycle_2/format_SHP_wfd_river_waterbody_catchments_cycle_2/SNAPSHOT_2021_03_17_wfd_river_waterbody_catchments_cycle_2/WFD_River_Water_Body_Catchments_Cycle_2.shp',

'/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_wfd_surface_water_management_catchments_cycle_2/format_SHP_wfd_surface_water_management_catchments_cycle_2/SNAPSHOT_2021_03_17_wfd_surface_water_management_catchments_cycle_2/WFD_Surface_Water_Management_Catchments_Cycle_2.shp',

'/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_wfd_surface_water_management_catchments_cycle_2/format_SHP_wfd_surface_water_management_catchments_cycle_2/SNAPSHOT_2021_03_17_wfd_surface_water_management_catchments_cycle_2/WFD_Surface_Water_Management_Catchments_Cycle_2.shp',

'/dbfs/mnt/base/unrestricted/source_ordnance_survey_data_hub/dataset_rivers_canals_streams_vector/format_SHP_rivers_canals_streams_vector/SNAPSHOT_2021_03_16_rivers_canals_streams_vector/WatercourseLink.shp',

]

# COMMAND ----------

woodland = [

'/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_community_forests/format_GPKG_community_forests/SNAPSHOT_2021_03_15_community_forests/refdata_owner.community_forest.gpkg',

'/dbfs/mnt/base/unrestricted/source_forestry_commission_open_data/dataset_national_forest_inventory_woodland_england_2020/format_SHP_national_forest_inventory_woodland_england_2020/SNAPSHOT_2022_10_19_national_forest_inventory_woodland_england_2020/National_Forest_Inventory_Woodland_England_2020.shp',

'/dbfs/mnt/base/unrestricted/source_forestry_commission_open_data/dataset_felling_licences/format_SHP_felling_licences/SNAPSHOT_2021_03_18_felling_licences/Felling_Licence_Applications_England.shp',

'/dbfs/mnt/base/unrestricted/source_englands_community_forests_partnership/dataset_community_forests/format_SHP_community_forests/SNAPSHOT_2023_01_13_community_forests/englands_community_forests.shp',

'/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_ancient_woodland_inventory/format_SHP_ancient_woodland_inventory/SNAPSHOT_2023_02_14_ancient_woodland_inventory/Ancient_Woodland_England.shp',

'/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_traditional_orchards/format_SHP_traditional_orchards/SNAPSHOT_2021_03_22_traditional_orchards/Traditional_Orchards_HAP_England.shp',

'/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_wood_pasture_and_parkland/format_SHP_wood_pasture_and_parkland/SNAPSHOT_2021_03_18_wood_pasture_and_parkland/Wood_Pasture_and_Parkland_BAP_Priority_Habitat_Inventory_for_England.shp',

]

# COMMAND ----------

from pyspark_vector_files.gpkg import read_gpkg


f_rpa_efa_hedges = '/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_efa_control_layer/format_GPKG_efa_control_layer/SNAPSHOT_2023_02_07_efa_control_layer/LF_CONTROL_MV.zip/LF_CONTROL_MV.gpkg'
f_hedges = '/dbfs/mnt/lab/unrestricted/elm_data/rpa/efa_hedges/2023_02_07.parquet'


sdf = read_gpkg(f_rpa_efa_hedges)
sdf.write.parquet(f_hedges)
