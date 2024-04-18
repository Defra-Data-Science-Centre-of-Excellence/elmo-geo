# Ingest

## New Ingestion Proposal
### Old Ingestion
- `process_datasets.py` all are documented in `datasets.py`
- manual ingestion to `elm_data` and `elm`
- manual attempt to move to a warehouse in `ELM-Project`
### New Ingestion
- Move `datasets.py` to `catalogue.json`
- Document catalogue source uri
- Eventually all data will be warehoused in `ELM-Project`
- List available data sources, list available data




## Methodology
1. List available data
2. Fetch and ingest data
3. Spatially join with parcels

### Data Formats
- Tabular: convert to parquet
- Vector: convert to geoparquet (encoding=WKB, CRS=BNG, precision=1mm, dims=2D, make_valid=True, order_rotation=True, sindex=BNG4)
- Raster: convert to parquet (encoding=tif, resolution=_variable_)

## Data
### Source
- DASH Governed: [dbfs/mnt/base/](dbfs:/mnt/base/)
- Defra Data Services Platform: https://environment.data.gov.uk/
- Defra GeoPortal: https://naturalengland-defra.opendata.arcgis.com/
- ONS GeoPortal: https://geoportal.statistics.gov.uk/
- Historic England GeoPortal: https://opendata-historicengland.hub.arcgis.com/
- OpenStreetMap: https://osm.org, http://tagfinder.osm.ch/
- Ordnance Survey: https://osdatahub.os.uk, https://osdatahub.os.uk/downloads/packages/2010
- Manual:
  - SharePoint
  - SCE S3

### Intermidiate
- `data/`: warehoused/ingested datasets, BNG spatially index, as parquet, with internal id  (rpa-hedge: id_boundary, valid_from, valid_to, geometry)
- `data/slookup` 12m spatial join lookup tables between geospatial datasets  (slookup-parcel_hedge: id_parcel, id_boundary)
- `data/prop` proportion of parcels covered by other feature



# TODO CSV
| name | path |
|---|---|
| rpa-parcel-adas_stg | /dbfs/mnt/lab/restricted/ELM-Project/stg/rpa-parcel-adas.parquet |
| rpa-parcel-adas | /dbfs/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet |
| rpa-parcel-2021_03_16 | /dbfs/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2021_03_16.parquet |
| rpa-parcel-2023_02_07 | /dbfs/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_02_07.parquet |
| rpa-parcel-2023_06_03 | /dbfs/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_06_03.geoparquet |
| rpa-parcel-2023_12_13 | /dbfs/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-2023_12_13.parquet |
| he-shine-2022_12_30 | /dbfs/mnt/lab/restricted/ELM-Project/stg/he-shine-2022_12_30.parquet |
| defra-alc-2024-02-27 | /dbfs/mnt/lab/unrestricted/elm_data/defra/alc/ALC_BMV_UNION_Merge_3b.shp |
| ewco-red_squirrel-2022_10_18 | /dbfs/mnt/lab/unrestricted/elm_data/ewco/red_squirrel/2022_10_18/EWCO_Biodiversity___Priority_Species___Red_Squirrel___Woodland_Creation.shp |
| ewco-priority_habitat_network-2022_10_06 | /dbfs/mnt/lab/unrestricted/elm_data/ewco/priority_habitat_network/2022_10_06/EWCO_Biodiversity___Priority_Habitat_Network.shp |
| ewco-nfc_social-2022_03_14 | /dbfs/mnt/lab/unrestricted/elm_data/ewco/nfc_social/2022_03_14/EWCO___NfC_Social.shp |
| ewco-water_quality-2023_02_27 | /dbfs/mnt/lab/unrestricted/elm_data/ewco/water_quality/2023_02_27/EWCO__E2_80_93_Water_Quality.shp |
| ewco-flood_risk_management-2023_02_24 | /dbfs/mnt/lab/unrestricted/elm_data/ewco/flood_risk_management/2023_02_24/EWCO___Flood_Risk_Management.shp |
| ewco-keeping_rivers_cool_riparian_buffers-2023_03_03 | /dbfs/mnt/lab/unrestricted/elm_data/ewco/keeping_rivers_cool_riparian_buffers/2023_03_03/EWCO___Keeping_Rivers_Cool_Riparian_Buffers.shp |
| ewco-nfc_ammonia_emmissions-2022_03_14 | /dbfs/mnt/lab/unrestricted/elm_data/ewco/nfc_ammonia_emmissions/2022_03_14/EWCO___NfC_Ammonia_Emissions_Capture_for_SSSI_Protection.shp |
| elmo_geo-baresoils | /dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/tiles.parquet |
| ons-regions-2021 | /dbfs/mnt/migrated-landing/Office of National Statistics/Regions__December_2021__EN_BFC.geojson |
| defra-national_parks-2021_03_15 | /dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_parks/format_SHP_national_parks/SNAPSHOT_2021_03_15_national_parks/National_Parks_England.shp |
| defra-national_parks-2023_02_14 | /dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_parks/format_SHP_national_parks/SNAPSHOT_2023_02_14_national_parks/National_Parks_England.shp |
| defra-ramsar-2021_03_16 | /dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_ramsar/format_SHP_ramsar/SNAPSHOT_2021_03_16_ramsar/Ramsar_England.shp |
| defra-ramsar-2023_02_14 | /dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_ramsar/format_SHP_ramsar/SNAPSHOT_2023_02_14_ramsar/Ramsar_England.shp |
| defra-peaty_soils | /dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_peaty_soils/format_GPKG_peaty_soils/SNAPSHOT_2021_03_24_peaty_soils/refdata_owner.peaty_soils_location.gpkg |
| defra-national_character_areas | /dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_character_areas/format_SHP_national_character_areas/SNAPSHOT_2021_03_29_national_character_areas/National_Character_Areas___Natural_England.shp |
| defra-sssi | /dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_sites_of_special_scientific_interest/format_SHP_sites_of_special_scientific_interest/SNAPSHOT_2021_03_17_sites_of_special_scientific_interest/Sites_of_Special_Scientific_Interest_England.shp |
| defra-sssi | /dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_sites_of_special_scientific_interest/format_SHP_sites_of_special_scientific_interest/SNAPSHOT_2023_02_14_sites_of_special_scientific_interest/Sites_of_Special_Scientific_Interest_England.shp |
| defra-aonb | /dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_areas_of_outstanding_natural_beauty/format_SHP_areas_of_outstanding_natural_beauty/SNAPSHOT_2021_03_15_areas_of_outstanding_natural_beauty/Areas_of_Outstanding_Natural_Beauty_England.shp |
| defra-aonb | /dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_areas_of_outstanding_natural_beauty/format_SHP_areas_of_outstanding_natural_beauty/SNAPSHOT_2023_02_14_areas_of_outstanding_natural_beauty/Areas_of_Outstanding_Natural_Beauty_England.shp |
| defra-moorline | /dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_lfa_and_moorland_line/format_GPKG_lfa_and_moorland_line/SNAPSHOT_2021_03_03_lfa_and_moorland_line/refdata_owner.lfa.gpkg |
| defra-moorline | /dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_lfa_and_moorland_line/format_GPKG_lfa_and_moorland_line/SNAPSHOT_2023_02_07_lfa_and_moorland_line/refdata_owner.lfa.zip/refdata_owner.lfa/refdata_owner.lfa.gpkg |
| rpa-commons-2021_03_05 | /dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_registered_common_land_bps_layer/format_GPKG_registered_common_land_bps_layer/SNAPSHOT_2021_03_05_registered_common_land_bps_layer/RCL.gpkg |
| rpa-commons-2023_02_07 | /dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_registered_common_land_bps_layer/format_GPKG_registered_common_land_bps_layer/SNAPSHOT_2023_02_07_registered_common_land_bps_layer/refdata_owner.rcl.zip/refdata_owner.rcl/refdata_owner.rcl.gpkg |
| defra-flood_risk_areas-2023_02_14 | /dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_flood_risk_areas/format_SHP_flood_risk_areas/SNAPSHOT_2023_02_14_flood_risk_areas/Flood_Risk_Areas.shp |
| defra-flood_risk_areas-2021_03_18 | /dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_flood_risk_areas/format_SHP_flood_risk_areas/SNAPSHOT_2021_03_18_flood_risk_areas/Flood_Risk_Areas.shp |