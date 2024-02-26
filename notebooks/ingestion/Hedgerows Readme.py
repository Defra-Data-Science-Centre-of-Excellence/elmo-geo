# Databricks notebook source
# MAGIC %md
# MAGIC # Hedgerows in England
# MAGIC Disclaimer: This is the understanding of Andrew.West@defra.gov.uk, please correct me.
# MAGIC
# MAGIC ### Questions
# MAGIC - What is the length of hedgerows in England?
# MAGIC     - Government Target 1984+10% = 630,000,000 m
# MAGIC     - This depends on dataset, but using RPA's data **420,000,000 m**
# MAGIC - What do we mean by length of hedgerow?
# MAGIC     - Length of a linear feature is obvious; `ST_Length(LineString)`.
# MAGIC     - But "length" of an area is less easy, `ST_Length(Polygon)` is the perimeter.
# MAGIC     - *I am researching centreline methodologies for converting Polygons to LineStrings...*
# MAGIC - How are hedgerows identified?
# MAGIC     - Directly; Site visits and AES agreements, drawing hedges on a map.
# MAGIC     - Height Classification; create a vegetation/tree map, then create linear features, uses aerial LiDAR.
# MAGIC     - Optical Classification; uses a statistical neural network or other CV model to identify patches of hedges, can use satellite photography.
# MAGIC - What classifications do we have?
# MAGIC     - EFA eligible hedgerows
# MAGIC     - Relict hedge
# MAGIC     - Tree line / linear woodland
# MAGIC     - HWS (Hedge, Wood, Scrub) - teagasc terminology
# MAGIC     - Wall / other heritage bufferables: Devon bank/Cornish hedge, dry stone wall, relict sites, archeological sites.
# MAGIC
# MAGIC - **Top 5 Hedgerow Projects in 2024**
# MAGIC     - **FR - NCEA: Trees Outside Woodland**
# MAGIC     - **RPA - Classified Hedges**
# MAGIC     - **OS - Field Boundaries**
# MAGIC     - **UKCEH - Land Cover Map Plus: Hedgerows**
# MAGIC     - elmo_geo - Sylvan *(identifies relict hedges)*
# MAGIC     - elmo_geo - Boundary *(merging all on parcels)*
# MAGIC
# MAGIC - Timeliness and Monitoring
# MAGIC     - NE - LiDAR was collected 2016-2021, I think new data is being collected.
# MAGIC     - I think these models are one shot for the time being, not monitoring sources.
# MAGIC
# MAGIC - Urban and Periurban Hedgerows
# MAGIC     - The hedgerow detection models will capture vegetation and height, which should work well near fields and open terrain, but will be more difficult in urban areas.
# MAGIC     - I'd expect to see hedgerows around a football pitch, but not along a terraced street, and gardens are dense and probably won't contain "linear features".
# MAGIC
# MAGIC - Tim Farron MP said ["[Britains hedgerows] would stretch to the moon and back"](https://parliamentlive.tv/event/index/b7ab5c1c-dbc6-4a16-8b7a-a74f4493c92d?in=10:08:40)?!
# MAGIC     - Only in England we have 420Mm of hedges, the average distance to the moon is 385Mm, so there and back is a stretch - pardon the pun.
# MAGIC     - Following, he did say "over half a million miles of hedgerows", which I imagine was just mis-speaking/reading meters as miles.
# MAGIC

# COMMAND ----------

# MAGIC %md ### Data Sources
# MAGIC | License | Source | Dataset | Version | Suitability | Methodology | Contact / Link | Format |
# MAGIC |---|---|---|---|---|---|---|---|
# MAGIC | Open | Sentinel | S2A MSI |  | Imagery | Satellite | https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-2 | Raster
# MAGIC | Restricted | NE | LiDAR | Collected 2016-21 + updates | Imagery | Aerial | https://www.data.gov.uk/dataset/f0db0249-f17b-4036-9e65-309148c97ce4/national-lidar-programme | Raster
# MAGIC | Restricted | Bluesky | Aerial Photography |  | Imagery | Aerial + Satellite |  | Raster
# MAGIC | Restricted | Bluesky | Tree Map | ? | ? | Similar to VOM - Tree Detection | https://bluesky-world.com/ntm/ | Point
# MAGIC | Open | EA | Vegetation Object Model | 2021 | poor | Raster, LiDAR post-processed to provide a canopy height model.  Excludes vegetation under 2.5m. | https://environment.data.gov.uk/dataset/ecae3bef-1e1d-4051-887b-9dc613c928ec |  Raster
# MAGIC | PSGA | OS | Master Map | Superceeded by NGD | little hedge data |  |  | Vector
# MAGIC | PSGA | OS | NGD |  | no hedge data |  |  | Vector
# MAGIC | Open | OSM | boundary:hedge |  | sparce data | crowd sourced |  | Vector
# MAGIC | Restricted | RPA | Hedge Managed | Superceeded by Control | fine | OS Field Boundaries + AES Agreements |  | Linear
# MAGIC | Restricted | RPA | Hedge Control |  | fine | Tidied RPA - Hedge Managed, eligible hedges only |  | Linear
# MAGIC | Restricted | RPA | Classified Hedges | WIP | best | AI image recognition model using Sentinel + NE - LiDAR + AES Agreements, verified against RPA - Hedge Control |  | Polygon
# MAGIC | Restricted | FR | NCEA: Trees Outside Woodland | V2  | fine | Bluesky National Tree Map plus classification of aerial photography.  Hedgerows over 20m long, under 4m wide, and under 3m tall included. 336,000 km of hedgerow in England. | Ben.Ditchburn@ncea.gov.uk, Freddie.Hunter@forestresearch.gov.uk | Polygon
# MAGIC | PSGA? | OS | Field Boundaries | WIP | ? | ? |  | Linear
# MAGIC | Restricted | UKCEH | Land Cover Map Plus: Hedgerows |  | ? | NE-LiDAR + UKCEH-LCM validated against "OS data" | [ceh-catalogue](https://catalogue.ceh.ac.uk/documents/d90a3733-2949-4dfa-8ac2-a88aef8699be) [ceh-description](https://www.ceh.ac.uk/press/high-tech-aerial-mapping-reveals-englands-hedgerow-landscape) | ? |
# MAGIC | Internal | elmo_geo | hedge | WIP |  | Joins hedges to parcels and assigns the parcel boundary as hedge. | Andrew.West@defra.gov.uk | Linear
# MAGIC | Internal | elmo_geo | sylvan | WIP | | Uses VOM - Tree Detection [pycrown](https://github.com/obisargoni/pycrown) algorithm | Obi.ThompsonSargoni@defra.gov.uk | Linear
# MAGIC | Internal | elmo_geo | boundaries | WIP |  | hedge plus other boundary features | Andrew.West@defra.gov.uk, Obi.ThompsonSargoni@defra.gov.uk | Linear
# MAGIC
# MAGIC ##### Nomenclature
# MAGIC - OS - Ordnance Survey
# MAGIC - PSGA - [Public Sector Geospatial Agreement](https://www.ordnancesurvey.co.uk/customers/public-sector/public-sector-licensing)
# MAGIC - NGS - National Geographic Database
# MAGIC - OSM - OpenStreetMap
# MAGIC - RPA - Rural Payments Agency
# MAGIC - FR - Forestry Research
# MAGIC - NCEA - Natural Capital and Ecosystem Assessment
# MAGIC - NE - Natural England

# COMMAND ----------

# MAGIC %md ### Total Hedgerows
# MAGIC |      | Total Length (m) | Explanation
# MAGIC | :--- | ---------------: | :---
# MAGIC | 1984 |      572,670,000 | Managed hedgerow in 1984, [EIP 2023](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1133967/environmental-improvement-plan-2023.pdf), "peak historic hedgerow"
# MAGIC | 2007 |      477,000,000 | Managed hedgerows in 2007, [CS 2007](https://www.ceh.ac.uk/sites/default/files/Countryside%20Survey%202007%20UK%20Headline%20Messages_Part2.pdf) *in Great Britain*
# MAGIC | EFA  |      640,341,286 | EFA hedgerows
# MAGIC | Adj  |  **419,882,012** | EFA hedgerows adjusted (half if a hedge is adjacent to another parcel)
# MAGIC | OSM  |      102,408,028 | OSM hedgerows
# MAGIC | OSMp |       73,510,661 | OSM hedgerows within parcels
# MAGIC | 110% |      630,000,000 | A target of 110% of managed hedgerow in 1984 by 2050
# MAGIC | +75  |      494,000,000 | A target of +75e6m of hedgerow, defined using Adj, by 2050
# MAGIC | +45  |      464,000,000 | A intermediate target of +45e6m of hedgerow by 2037
# MAGIC | Adj 2023 |  575,100,528 | Recalculated using RPA - Hedge Control - 2023_12_18
# MAGIC | TOW 2023 |  336,000,000 | FR - NCEA: Trees Outside Woodland model's hedgerow features
# MAGIC | CEH 2024 |  390,000,000 | UKCEH - Land Cover Map Plus: Hedgerows, [press release](https://www.ceh.ac.uk/press/high-tech-aerial-mapping-reveals-englands-hedgerow-landscape) figure
# MAGIC
# MAGIC This table was originally created Dec 2022

# COMMAND ----------

# MAGIC %md ### elmo_geo Projects
# MAGIC - Spatial Features
# MAGIC   - Moorline: `ne-moorline`, `elmo-moor`
# MAGIC   - ...
# MAGIC - Sylvan
# MAGIC   - Relict Hedgerows: `elmo_geo-relict_hedgerow`
# MAGIC - Boundaries
# MAGIC   - Hedgerows: `elmo_geo-hedge` - [old notebook](https://adb-7422054397937474.14.azuredatabricks.net/?o=7422054397937474#notebook/3191305210626420)
# MAGIC   - Waterbodies: `elmo_geo-water`
# MAGIC   - Heritage Walls: `elmo_geo-heritage`
# MAGIC   - Metrics: `elmo-boundary`
# MAGIC

# COMMAND ----------

# MAGIC %md ### Interested People
# MAGIC - Defra.ELM.DS - Andrew.West@defra.gov.uk
# MAGIC - Defra.ELM.GHD - Obi.ThompsonSargoni@defra.gov.uk
# MAGIC - RPA.DataMart - Andrew.Osborn@rpa.gov.uk
# MAGIC - RPA.Geospatial - [Brian.O'Toole@rpa.gov.uk](mailto:Brian.O'Toole@rpa.gov.uk)
# MAGIC - RPA.Hegdes - Yajnaseni.Palchowdhuri@rpa.gov.uk
# MAGIC - Defra.TreeTeam - Iain Dummett, Chris McGurk, Rory Lunny
# MAGIC - Protected Landscapes - Liz Bingham
