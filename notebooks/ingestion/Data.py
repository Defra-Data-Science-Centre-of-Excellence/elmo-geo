# Databricks notebook source
# MAGIC %md
# MAGIC # Data Preparation
# MAGIC
# MAGIC ### Data Format
# MAGIC - Tabular:  Parquet  (Spark)
# MAGIC - Vector:  Parquet (SedonaType Geometries, CRS=EPSG:27700, Precision=1mm, 2D)
# MAGIC - Raster:  GeoTIFF?
# MAGIC
# MAGIC ### Governed Data
# MAGIC Data from the governed area is **converted** to our standardised data formats, using the `elm_se.io.ingest` method.
# MAGIC
# MAGIC ### OS Data
# MAGIC Select + Build is used to create the recipe "[Everything](https://osdatahub.os.uk/downloads/recipes/1891)" from OS NGD is dated 2023-05-22.  John Joseph from open@defra.gov.uk, helped create the **download**: [id_product=2010](https://osdatahub.os.uk/downloads/packages/2010).  The data is zipped geopackages, with some very large files.  The files are downloaded using `osdatahub.DownloadAPI`, then converted using `elm_se.io.ingest`, along with dataframe `success.csv` recording conversion status for each dataset.  Original format files are deleted.  
# MAGIC Open OS data is downloaded using the same API, but these products are not part of the NGD database.  
# MAGIC Further note is that FeaturesAPI and others are using the old database not NGD.  
# MAGIC
# MAGIC ### OSM Data
# MAGIC The required data is downloaded separately using `osmnx` with key-tags found using [tagfinder](http://tagfinder.osm.ch/).  
# MAGIC

# COMMAND ----------

# DBTITLE 1,Governed data conversion 
import geopandas as gpd


datasets = [
  ['/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_reference_parcels/format_GPKG_rpa_reference_parcels/SNAPSHOT_2023_06_03_rpa_reference_parcels/reference_parcels.zip/reference_parcels/reference_parcels.gpkg', '/dbfs/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_06_03.geoparquet'],
  ['/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_efa_control_layer/format_GPKG_efa_control_layer/SNAPSHOT_2023_06_27_efa_control_layer/LF_CONTROL_MV.zip/LF_CONTROL_MV/LF_CONTROL_MV.gpkg', '/dbfs/mnt/lab/unrestricted/elm_data/rpa/efa_hedge/2023_06_27.geoparquet'],
]


for f_in, f_out in datasets:
  gpd.read_file(f_in, engine='pyogrio').to_parquet(f_out)


# COMMAND ----------

# DBTITLE 1,OSM data ingestation
# import elm_se
# elm_se.register()
# from elm_se.io import ingest_osm

import osmnx
osmnx.settings.cache_folder = '/databricks/driver/'
osmnx.settings.timeout = 600


def ingest_osm(f, place, tags):
  gdf = (osmnx.features_from_place(place, tags)
    .reset_index()
    [['osmid', *tags.keys(), 'geometry']]
  )
  gdf.to_parquet(f)
  return gdf



datasets = [
  {  # Hedgerow
    'f': '/dbfs/mnt/lab/unrestricted/elm_data/osm/hedgerow.parquet',
    'place': 'England',
    'tags': {
      'barrier': ['hedge', 'hedge_bank'],
      'landcover': 'hedge',
    }
  },
  {  # Waterbody
    'f': '/dbfs/mnt/lab/unrestricted/elm_data/osm/waterbody.parquet',
    'place': 'England',
    'tags': {
      'water': True,
      'waterway': True,
      'drain': True,
    }
  },
  {  # Heritage Wall
    'f': '/dbfs/mnt/lab/unrestricted/elm_data/osm/heritage_wall.parquet',
    'place': 'England',
    'tags': {
      'wall': 'dry_stone',
    }
  },
]


for dataset in datasets:
  print(dataset['tags'])
  ingest_osm(**dataset)
  print(dataset['f'])


# COMMAND ----------

# MAGIC %cat /dbfs/databricks/logs/0630-082601-sn72lnle/init_scripts/0630-082601-sn72lnle_10_178_52_219/20230731_110445_00_DASH.sh.stderr.log

# COMMAND ----------

# DBTITLE 1,OS NGD data ingestation
import os
from glob import glob
import geopandas as gpd

# import elm_se
# elm_se.register(spark)
# from elm_se.io import dbfs, is_ingested, ingest_file

# import osdatahub


path = '/dbfs/mnt/lab/unrestricted/elm_data/os/mmtopo/'


# key = 'WxgUdETn6cy58WZkfwZ7wdMVLlt5eDsX'
# product_id = '2010'
# version_id = '6882'
# osdatahub.DataPackageDownload(key, product_id).download(version_id, path)


# os.system(f'cd {path} && find *.zip | parallel unzip -n')


bans = [
  'add_gb_builtaddress',
  'bld_fts_buildingline',
  'bld_fts_buildingpart',
  'lnd_fts_land',
  'lus_fts_site',
  'lus_fts_siteaccesslocation',  # missing
  'str_fts_compoundstructure',  # datetime
  'str_fts_structureline',
  'trn_fts_roadtrackorpath',
  'trn_ntwk_roadlink',
  'trn_rami_averageandindicativespeed',
]

LIMIT = 10  # GiB
for f_in in glob(path+'*.gpkg'):
  size = round(os.path.getsize(f_in) / 1024**3, 1)
  print(size, f_in, sep='\t')
  f_out = f_in.replace('.gpkg', '.geoparquet')
  if not os.path.exists(f_out) and not any(ban in f_in for ban in bans):
    if size < LIMIT:
      gpd.read_file(f_in, engine='pyogrio').to_parquet(f_out)
      print('\t', f_out, sep='\t')
    else:
      print('\t', f_in.split('/')[-1].split('.')[0], sep='\t')


# os.system(f'cd {path} && rm *.json && rm *.zip && rm *.gpkg)

# COMMAND ----------

# DBTITLE 1,Other Sources
url_countries = 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Countries_December_2022_UK_BFC/FeatureServer/0/query?where=1%3D1&outFields=CTRY22NM&outSR=27700&f=json'
