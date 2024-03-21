# ELM SE
Environmental Land Management (ELM) Spatial Eligibility (SE) parcel level data creation.
This project aims to add geospatial data to the information known about parcel to feed into ELM's population model.

### To Do
- move notebooks across
- create and link notebooks (I think each notebook can be a tree that links to more analysis)
- continue with all known relevent datasets
- add source data links
- bng index function


### Source Data
| Category | Dataset | Link | Description |
|---|---|---|---|
| Parcel | RPA, Reference Parcels | ... | ... (include origin e.g. `dbfs:/base/rpa/parcel/version.gpkg`) |
| Hedgerow | RPA, EFA Control Layer (hedges) | ... | ... |
| Hedgerow | OSM, Hedgerows | ... | ... |
| Hedgerow, Woodland | EA, LiDAR VOM | ... | ... |
| Hedgerow, Woodland | FR, Trees Outside Woodland | ... | trees canopy and crown, linear/group/single | 
| Waterbody, Woodland | OS, NGD | ... | [https://osdatahub.os.uk/downloads/packages/2010]()
| Heritage Wall | OSM, Heritage Wall |
| Country | ONS, Country |
| Urban | OS |
| Urban | Defra |



### Intermidiate Data
| Status | Dataset | Link | Schema | Description |
| --- | --- | --- | --- | --- |
| ❌ | Parcel | ... | id_business, id_parcel, geometry | converted |
| ❌ | OSM_Heritage_Wall | ... | id_parcel, id, type, geometry | converted, and joined |
| ❌ | OS_Waterbody | ... | ^ | converted, and joined |
| ❌ | OSM_Waterbody | ... | ^ | converted, and joined |
| ❌ | ?_Woodland | ... | ^ | ... |
| ❌ | RPA_Hedgerow | ... | ^ | converted, and joined |
| ❌ | OSM_Hedgerow | ... | ^ | converted, and joined |
| ❌ | Relict_Hedgerow | ... | ^ | ea/fr lidar used to identify relict hedgerows |
| ❌ | Boundary | ... | id_parcel, id, group, type, geometry | unified boundary data |
| ❌ | ONS_Country | ... | ... | ... |
| ❌ | OS_Urban | ... | ... | urban geometries, converted and joined |
| ❌ | Defra_Urban | ... | ... | ^ |


*type is mainly relavent for Boundary, but for os data it would be something like description, which for waterbody would include drain.*


### Output Data
| Status | Dataset | Link | Schema | Description |
| --- | --- | --- | --- | --- |
| ❌ | Heritage Wall Statistics | ... | ... | ... |
| ❌ | Waterbody Statistics | ... | ... | ... |
| ❌ | Hedgerow Statistics | ... | ... | ... |
| ❌ | Woodland Statistics | ... | ... | ... |
| ❌ | Boundary Statistics | ... | id_parcel, group, type, /stats/ | unified, statistics at each buffer |
| ❌ | Urban | ... | id_parcel, urban | urban status |


### Notebooks
| Status | Notebook | Task |
|---|---|---|
| ❌ | [Data](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/4426024615916685/) | Methodology for the preparation all source data, including downloading [OSM]() and [OS]() data. |
| ❌ | [Heritage Wall](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/1370439409529193/) | Easiest task: summary, dl OSM & convert, analysis. Summary: Table comparing Data Sources, Table of Summary Statistics, data decision.  Types: Dry Stone, Cornish |
| ❌ | [Waterbody](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/1370439409529195/) | Medium task: summary, also OS data, same analysis. Includes drains.  Types: Waterbody, Drain |
| ❌ | [Woodland](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/1370439409529197/) | Very Hard Task: requires LiDAR data, but also comparing with other validated datasources.  Including LiDAR conversion.  Types: Commercial, Ammenity, Native, Ancient |
| ❌ | [Hedgerow](https://adb-7393756451346106.6.azuredatabricks.net/?o=7393756451346106#notebook/2356646032716084/) | Hard task: summary, also RPA and LiDAR data, relict hedgerows requires extra analysis.  Types: EFA, Relict |
| ❌ | [Boundary?]() | Medium task: requires unifying all the previous datasets as "group", and calculating the summary statistics for different buffers. |
| ❌ | [Country borders Hedgerow]() | ... |
| ❌ | [Urban]() | ... |


### Methology

##### Join methodology
![Figure of Buffered Parcel Join]()


##### Statistics Methodology


### Extras

##### Tips
[TagFinder](tagfinder.osm.ch)
Check out my repo [cdap_geo](https://github.com/aw-west-defra/cdap_geo) for UDF, Sedona and general GIS on DASH help.  
I recommend Sedona on Clusters: 3_GeoVector, 3a_GeoHC. [api](https://sedona.apache.org/latest-snapshot/api/sql/Function/), [tips](https://github.com/aw-west-defra/cdap_geo/blob/main/cdap_geo/sedona.py)  

##### Found Bugs
Use `ST_ReducePrecision(g, 0.001)` (1mm grid) - [gis#50399](https://gis.stackexchange.com/q/50399)  
Use `ST_Buffer(ST_Buffer(g, 0.001), buf)` (1mm pre-buffer) - [jts#876](https://github.com/locationtech/jts/issues/876)  
OSMnx doc issue - [osmnx#931](https://github.com/gboeing/osmnx/issues/931)  
`SedonaDF.toPandas` only works for `id+geometry` not more columns.  
OSMnx requires tmp folder if in a repo: `ox.settings.cache_folder = '/dbfs/tmp/'`  
`SparkSession` requires to be called from a notebook, `databricks.sdk.runtime` will only be available in DBR 13+

##### Status
✔️⚙️❌
