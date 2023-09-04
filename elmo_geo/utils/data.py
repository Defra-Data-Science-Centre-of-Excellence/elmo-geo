# data lake
base = 'dbfs/mnt/base/source/dataset/format/version/'

# data warehouse
'dbfs/mnt/elm/data/source/dataframe/version.parquet/' # < 
'dbfs/mnt/elm/data/source/dataframe/version.geoparquet/'
'dbfs/mnt/elm/data/source/dataframe/version.raster/'
# example
'dbfs/mnt/elm/data/rpa/reference_parcels/2023_08.geoparquet'  # not a parquet directory < ingestion only
'dbfs/mnt/elm/data/rpa/reference_parcels/2023_08.parquet/sindex=SP0213'  # drop area/length columns


'dbfs/mnt/elm/bare_soils/parcel_bareness/2023_08.parquet/sindex=SP0213'

'dbfs/mnt/elm/raw/sentile/?/?.raster/sindex=SP0213'
'dbfs/mnt/elm/raw/os_ngd/wtr_fts_water/2023_07_01.parquet/sindex=SP0213'
'dbfs/mnt/elm/raw/os_ngd/wtr_fts_water/2023_07_01.parquet/part_0'


# ewkb < crs,bbox

# dbfs/mnt/elm/lnd/  .geoparquet / .geojson / .osm  # 
# dbfs/mnt/elm/ods/  .parquet / .raster  # operational datastore
# dbfs/mnt/elm/parcel_join/
# dbfs/mnt/elm/bare_soils/
# dbfs/mnt/elm/sylvan/
