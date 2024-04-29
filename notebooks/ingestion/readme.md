# Ingest

## Method
1. `01_identify`, Identify datasets, manually adding the "dataset" to `data/catalogue.json`.
2. `02_convert`, Convert datasets to be easily access by spark, partitioning large datasets approriately.
3. `03_lookup`, Spatially join with parcels, using distance, KNN, and metrics if declared in dataset, saving just lookup tables.

## Data Sources
A list of places that have been used to identify datasets.
| Source | Link |
| ------ | ---- |
DASH (PowerBI) | https://app.powerbi.com/groups/me/apps/5762de14-3aa8-4a83-92b3-045cc953e30c
data.gov.uk | https://data.gov.uk
MAGIC | https://magic.defra.gov.uk/Dataset_Download_Summary.htm
CKAN | https://ckan.publishing.service.gov.uk/dataset/
ONS | https://geoportal.statistics.gov.uk/<br>API: https://services.arcgis.com/JJzESW51TqeY9uat
NE-Defra | https://naturalengland-defra.opendata.arcgis.com/<br>API: https://services1.arcgis.com/ESMARspQHYMw9BZ9
Historic England | https://opendata-historicengland.hub.arcgis.com/<br>API: https://services-eu1.arcgis.com/ZOdPfBS3aqqDYPUQ
Forestry | https://data-forestry.opendata.arcgis.com/<br>API: https://services2.arcgis.com/mHXjwgl3OARRqqD4/
UKCEH | https://eidc.ac.uk/<br>https://catalogue.ceh.ac.uk/eidc/documents
