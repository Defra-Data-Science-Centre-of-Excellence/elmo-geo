# elmo analysis on CDAP


### Input Data

[Convert Base to Parquet](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/2186151298827810/)
- WFM, Parcels
- RPA, Reference Parcels
- RPA, EFA Hedge
- OS, Watercourse

[OSM Download to Parquet](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/4086960225424680/)
- OSM, Hedgerows
- OSM, Waterbodies
- OSM, Heritage


### Buffer Strips
- [Parcels](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/2186151298827814)
`dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet`
- [Hedgerows](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/2186151298827812)
`dbfs:/mnt/lab/unrestricted/elm/buffer_strips/hedgerows.parquet`
- [Waterbodies](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/3068502344869245)
- [Walls](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/592255967101153)

- [parcel_overlap](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/1008623224844238)
- [woodland data](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/3169394950173567)
- [Parcel 2023 vs 2021](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/2182312242921951/command/2182312242921952)
- [RPA, Parcels](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/1797364643665911)
- [Hedgerow Overlaps](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/2110612226901949)

- [Buffer Strips Join](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/4177208681924841)
- [Buffer Strips Area](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/3169394950173604)
- [Buffer Strips Use](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/4447004434027675)

### Output Data
- `s3://s3-ranch-013/data/elmo/from_cdap/hedgerows_and_waterbodies-v3.csv`



# Extras

### Links
- OSM [tag finder](http://tagfinder.herokuapp.com/)  
- Countryside Survey, [part 1](https://www.ceh.ac.uk/sites/default/files/Countryside%20Survey%202007%20UK%20Headline%20Messages_Part1.pdf), [part 2](https://www.ceh.ac.uk/sites/default/files/Countryside%20Survey%202007%20UK%20Headline%20Messages_Part2.pdf)
- [hedgelink.org.uk](https://hedgelink.org.uk/)
- [Environmental Improvement Plan](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1133967/environmental-improvement-plan-2023.pdf) pg44-4


### Tips
Check out my repo [cdap_geo](https://github.com/aw-west-defra/cdap_geo) for UDF, Sedona and general GIS on DASH help.  
I recommend Sedona on Clusters: 3_GeoVector, 3a_GeoHC. [api](https://sedona.apache.org/latest-snapshot/api/sql/Function/), [tips](https://github.com/aw-west-defra/cdap_geo/blob/main/cdap_geo/sedona.py)  


### Found Bugs
Use `ST_ReducePrecision(g, 0.001)` (1mm grid) - [gis#50399](https://gis.stackexchange.com/q/50399)  
Use `ST_Buffer(ST_Buffer(g, 0.001), buf)` (1mm pre-buffer) - [jts#876](https://github.com/locationtech/jts/issues/876)  
OSMnx doc issue - [osmnx#931](https://github.com/gboeing/osmnx/issues/931)  
`SedonaDF.toPandas` only works for `id+geometry` not more columns.  
OSMnx requires tmp folder if in a repo: `ox.settings.cache_folder = '/dbfs/tmp/'`  