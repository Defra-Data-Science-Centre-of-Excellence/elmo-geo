BATCHSIZE:int = 10_000

FOLDER_CONDA:str = '/dbfs/databricks/miniconda/bin/'
FOLDER_STG:str = '/dbfs/mnt/lab/restricted/ELM-Project/stg'  # data staging - lake
FOLDER_ODS:str = '/dbfs/mnt/lab/restricted/ELM-Project/ods'  # operational data storage - warehouse

URL_NE:str = 'https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services'
URL_ONS:str = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/ArcGIS/rest/services'

FORMAT_LAKE:str = 'base/{source}/{dataset}/{format}/{version}/'
FORMAT_WAREHOUSE:str = 'elm/{source}/{dataset}/{version}.parquet'

LINK_BNG:str = 'https://britishnationalgrid.uk/'
LINK_GeoHash:str = 'https://geohash.softeng.co/gc'
LINK_H3:str = 'https://h3geo.org/'
LINK_S2:str = 'https://s2geometry.io/'
