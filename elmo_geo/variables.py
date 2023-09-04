CONDA_FOLDER:str = '/dbfs/databricks/miniconda/bin/'
STG_FOLDER:str = '/dbfs/mnt/lab/restricted/ELM-Project/stg'  # data staging
OGS_FOLDER:str = '/dbfs/mnt/lab/restricted/ELM-Project/ogs'  # data warehouse
BATCHSIZE:int = 10_000
NE_URL:str = 'https://services.arcgis.com/JJzESW51TqeY9uat/ArcGIS/rest/services'
ONS_URL:str = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/ArcGIS/rest/services'

FORMAT_LAKE:str = 'base/{source}/{dataset}/{format}/{version}/'
FORMAT_WAREHOUSE:str = 'elm/{source}/{dataset}/{version}.parquet'
