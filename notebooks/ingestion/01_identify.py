# Databricks notebook source
# MAGIC %md
# MAGIC # Identify
# MAGIC Identify datasets not in DASH governed data area.
# MAGIC 1.  Fetch them manually and upload them to the bronze area.
# MAGIC 2.  Add their catalogue entry rather than manually to the catalogue.
# MAGIC 3.  Run `add_to_catalogue`, 

# COMMAND ----------

from elmo_geo import register
from elmo_geo.datasets.catalogue import add_to_catalogue

register()

# COMMAND ----------

manual_datasets = [
    # RPA EVAST edition 
    {
        "name": "rpa-parcel-adas",
        "columns": {
            "Shape": "geometry"
        },
        "tasks": {
            "convert": "todo"
        },
        "note": "rpa-parcel-adas is the dataset used by EVAST (UKCEH), it is similar to WFM but not identical (noted 2024-05-15).  Access from UKCEH SharePoint (Defra Restricted Area).",
        "link": "https://cehacuk.sharepoint.com/sites/EVAST/DEFRA Data Share/",
        "bronze": "/dbfs/mnt/lab/restricted/ELM-Project/bronze/rpa-parcel-adas.parquet",
    },
    {
        "name": "rpa-hedge-adas",
        "columns": {
            "geom": "geometry"
        },
        "tasks": {
            "convert": "todo"
        },
        "note": "rpa-hedge-adas is RPA's control layer for hedgerows at a similar time as rpa-parcel-adas (Nov 2021).  Access from Defra SharePoint (ELM-RPA Restricted area).",
        "link": "https://defra.sharepoint.com/teams/Team1645/Restricted_ELM_RPA_data_sharing/",
        "bronze": "/dbfs/mnt/lab/restricted/ELM-Project/bronze/rpa-hedge-adas.parquet"
    },
    
    # EWCO
    {
        "name": "ewco-priority_habitat_network-2022_10_06",
        "columns": {
            "csht_pnts": "csht_pnts",
            "cswcm_pnts": "cswcm_pnts",
            "ewco_val": "ewco_val",
            "cat": "cat",
            "geometry": "geometry"
        },
        "tasks": {
            "convert": "2024_05_14",
            "lookup_knn_parcel": "todo"
        },
        "bronze": "/dbfs/mnt/lab/unrestricted/elm_data/ewco/priority_habitat_network/2022_10_06/EWCO_Biodiversity___Priority_Habitat_Network.shp",
        "silver": "/dbfs/mnt/lab/restricted/ELM-Project/silver/ewco-priority_habitat_network-2022_10_06.parquet"
    },
    {
        "name": "ewco-nfc_social-2022_03_14",
        "columns": {
            "status": "status",
            "geometry": "geometry"
        },
        "tasks": {
            "convert": "2024_05_14"
        },
        "bronze": "/dbfs/mnt/lab/unrestricted/elm_data/ewco/nfc_social/2022_03_14/EWCO___NfC_Social.shp",
        "silver": "/dbfs/mnt/lab/restricted/ELM-Project/silver/ewco-nfc_social-2022_03_14.parquet"
    },
    {
        "name": "ewco-water_quality-2023_02_27",
        "columns": {
            "cat": "cat",
            "geometry": "geometry"
        },
        "tasks": {
            "convert": "2024_05_14"
        },
        "bronze": "/dbfs/mnt/lab/unrestricted/elm_data/ewco/water_quality/2023_02_27/EWCO__E2_80_93_Water_Quality.shp",
        "silver": "/dbfs/mnt/lab/restricted/ELM-Project/silver/ewco-water_quality-2023_02_27.parquet"
    },
    {
        "name": "ewco-flood_risk_management-2023_02_24",
        "columns": {
            "LANDSCAPE": "landscape",
            "geometry": "geometry"
        },
        "tasks": {
            "convert": "2024_05_14"
        },
        "bronze": "/dbfs/mnt/lab/unrestricted/elm_data/ewco/flood_risk_management/2023_02_24/EWCO___Flood_Risk_Management.shp",
        "silver": "/dbfs/mnt/lab/restricted/ELM-Project/silver/ewco-flood_risk_management-2023_02_24.parquet"
    },
    {
        "name": "ewco-keeping_rivers_cool_riparian_buffers-2023_03_03",
        "columns": {
            "OBJECTID": "OBJECTID",
            "geometry": "geometry"
        },
        "tasks": {
            "convert": "2024_05_14"
        },
        "bronze": "/dbfs/mnt/lab/unrestricted/elm_data/ewco/keeping_rivers_cool_riparian_buffers/2023_03_03/EWCO___Keeping_Rivers_Cool_Riparian_Buffers.shp",
        "silver": "/dbfs/mnt/lab/restricted/ELM-Project/silver/ewco-keeping_rivers_cool_riparian_buffers-2023_03_03.parquet"
    },
    {
        "name": "ewco-nfc_ammonia_emmissions-2022_03_14",
        "columns": {
            "status": "status",
            "pnts": "pnts",
            "geometry": "geometry"
        },
        "tasks": {
            "convert": "2024_05_14"
        },
        "bronze": "/dbfs/mnt/lab/unrestricted/elm_data/ewco/nfc_ammonia_emmissions/2022_03_14/EWCO___NfC_Ammonia_Emissions_Capture_for_SSSI_Protection.shp",
        "silver": "/dbfs/mnt/lab/restricted/ELM-Project/silver/ewco-nfc_ammonia_emmissions-2022_03_14.parquet"
    },
    {
        "name": "ewco-red_squirrel-2022_10_18",
        "columns": {
            "status": "status",
            "cswcm_pnts": "cswcm_pnts",
            "ewco_val": "ewco_val",
            "sitename": "sitename",
            "cat": "cat",
            "geometry": "geometry"
        },
        "tasks": {
            "convert": "2024_05_14"
        },
        "bronze": "/dbfs/mnt/lab/unrestricted/elm_data/ewco/red_squirrel/2022_10_18/EWCO_Biodiversity___Priority_Species___Red_Squirrel___Woodland_Creation.shp",
        "silver": "/dbfs/mnt/lab/restricted/ELM-Project/silver/ewco-red_squirrel-2022_10_18.parquet"
    },
    
    # Other
    {
        "name": "sentinel-tiles-2023_02_07",
        "columns": {
            "Name": "tile",
            "geometry": "geometry"
        },
        "tasks": {
            "convert": "2024_05_14"
        },
        "bronze": "/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/tiles.parquet",
        "silver": "/dbfs/mnt/lab/restricted/ELM-Project/silver/sentinel-tiles-2023_02_07.parquet"
    },
    {
        "name": "he-shine-2022_12_30",
        "columns": {
            "Name": "tile",
            "shine_uid": "shine_uid",
            "shine_name": "shine_name",
            "shine_form": "shine_form",
            "significan": "significance",
            "web_url": "web_url",
            "last_edit": "last_edit",
            "priority": "priority",
            "geom": "geometry"
        },
        "tasks": {
            "convert": "2024_05_14"
        },
        "bronze": "/dbfs/mnt/lab/restricted/ELM-Project/bronze/he-shine-2022_12_30.parquet",
        "silver": "/dbfs/mnt/lab/restricted/ELM-Project/silver/he-shine-2022_12_30.parquet"
    },
    {
        "name": "defra-priority_habitat_inventory-2021_03_26",
        "columns": {
            "Main_Habit": "Main_Habit",
            "Confidence": "Confidence",
            "Source1":"Source1",
            "Source2":"Source2",
            "S2Habclass":"S2Habclass",
            "S2HabType":"S2HabType",
            "Source3":"Source3",
            "S3Habclass":"S3Habclass",
            "S3HabType":"S3HabType",
            "geometry":"geometry"
        },
        "tasks": {
            "convert": "todo"
        },
        "bronze": "/dbfs/mnt/lab/unrestricted/elm_data/defra/priority_habitat_inventory/unified_2021_03_26.parquet"
    },
    {
        "name": "living_england-habitat_map-2022_09_16",
        "columns": {
            "A_pred":"A_pred",
            "A_prob":"A_prob",
            "geometry":"geometry"
        },
        "tasks": {
            "convert": "todo"
        },
        "bronze": "dbfs:/mnt/lab/unrestricted/elm_data/natural_england/living_england/2022_09_16.parquet"
    }
]

# COMMAND ----------

add_to_catalogue(manual_datasets)
