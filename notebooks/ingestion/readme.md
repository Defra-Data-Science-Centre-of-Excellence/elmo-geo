# Ingest
- `02_convert`, Convert datasets to be easily access by spark, partitioning large datasets approriately.
- (work in progress) `03_lookup`, Spatially join with parcels, using distance, KNN, and metrics if declared in dataset, saving just lookup tables.

## Data Sources
A list of places that have been used to identify datasets.
| Source | Link |
| ------ | ---- |
DASH (PowerBI) | https://app.powerbi.com/groups/me/apps/5762de14-3aa8-4a83-92b3-045cc953e30c<br>API: [dbfs/mnt/base/]()
data.gov.uk | https://data.gov.uk
MAGIC | https://magic.defra.gov.uk/Dataset_Download_Summary.htm
CKAN | https://ckan.publishing.service.gov.uk/dataset/
ONS | https://geoportal.statistics.gov.uk/<br>API: https://services.arcgis.com/JJzESW51TqeY9uat
NE-Defra | https://naturalengland-defra.opendata.arcgis.com/<br>API: https://services1.arcgis.com/ESMARspQHYMw9BZ9
Historic England | https://opendata-historicengland.hub.arcgis.com/<br>API: https://services-eu1.arcgis.com/ZOdPfBS3aqqDYPUQ
Forestry Commission | https://data-forestry.opendata.arcgis.com/<br>API: https://services2.arcgis.com/mHXjwgl3OARRqqD4/
UKCEH | https://eidc.ac.uk/<br>https://catalogue.ceh.ac.uk/eidc/documents
Defra DSP | https://environment.data.gov.uk/
OpenStreetMap | https://osm.org, http://tagfinder.osm.ch/
Ordnance Survey | https://osdatahub.os.uk

## Manual Collected Datasets
dataset | link
--- | ---
rpa-parcel-adas | https://cehacuk.sharepoint.com/sites/EVAST/DEFRA%20Data%20Share/
rpa-hedge-adas | https://defra.sharepoint.com/teams/Team1645/Restricted_ELM_RPA_data_sharing/
he-shine-2022_12_30 | https://defra.sharepoint.com/teams/Team1645/Restricted_ELM_RPA_data_sharing/
sentinel-tiles-2023_02_07 | to replace with `dbfs:/mnt/eods`
ewco-priority_habitat_network-2022_10_06
ewco-nfc_social-2022_03_14
ewco-water_quality-2023_02_27
ewco-flood_risk_management-2023_02_24
ewco-keeping_rivers_cool_riparian_buffers-2023_03_03
ewco-nfc_ammonia_emmissions-2022_03_14
ewco-red_squirrel-2022_10_18

