from elmo_geo import register
register()

from elmo_geo.st.join import sjoin, sjoin_with_tree



sf_parcels = 'dbfs:/mnt/lab/restricted/ELM-Project/data/rpa-rpa_reference_parcels-2023_08_03.parquet'
sf_peat = 'dbfs:/mnt/lab/restricted/elm_data/defra-peaty_soils-2021_03_24.parquet'






sdf_parcels = spark.read.format('geoparquet').load(sf_parcels)
sdf_peat = spark.read.format('geoparquet').load(sf_peat)

display(sdf_parcels)
display(sdf_peat)
