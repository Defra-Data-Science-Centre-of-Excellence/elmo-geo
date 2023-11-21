# requires: gdal>3.5, p7zip-full

mkdir -p data_sync
for f in "rpa-parcels-2023_11_13" "rpa-hedge-2023_11_13" "rpa-hedge-adas"; do
    # ogr2ogr -f Parquet data/$f.parquet data/$f.gpkg -skipfailures
    7z a -v100m -mmt data_sync/$f.7z data/$f.parquet
done
make dbx
