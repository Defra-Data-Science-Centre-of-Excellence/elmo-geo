# Hedgerows in England
Disclaimer: This is the understanding of Andrew.West@defra.gov.uk, please correct me.

### Questions
- What is the length of hedgerows in England?
    - Government Target 1984+10% = 630,000,000 m
    - This depends on dataset, but using RPA's data **420,000,000 m**
- What do we mean by length of hedgerow?
    - Length of a linear feature is obvious; `ST_Length(LineString)`.
    - But "length" of an area is less easy, `ST_Length(Polygon)` is the perimeter.
    - *I am researching centreline methodologies for converting Polygons to LineStrings...*
- How are hedgerows identified?
    - Directly; Site visits and AES agreements, drawing hedges on a map.
    - Height Classification; create a vegetation/tree map, then create linear features, uses aerial LiDAR.
    - Optical Classification; uses a statistical neural network or other CV model to identify patches of hedges, can use satellite photography.
- What classifications do we have?
    - EFA eligible hedgerows
    - Relict hedge
    - Tree line / linear woodland
    - HWS (Hedge, Wood, Scrub) - teagasc terminology
    - Wall / other heritage bufferables: Devon bank/Cornish hedge, dry stone wall, relict sites, archeological sites.

- **Top 5 Hedgerow Projects in 2024**
    - **FR - NCEA: Trees Outside Woodland**
    - **RPA - Classified Hedges**
    - **OS - Field Boundaries**
    - **UKCEH - Land Cover Map Plus: Hedgerows**
    - elmo_geo - Sylvan *(identifies relict hedges)*
    - elmo_geo - Boundary *(merging all on parcels)*

- Timeliness and Monitoring
    - NE - LiDAR was collected 2016-2021, I think new data is being collected.
    - I think these models are one shot for the time being, not monitoring sources.

- Urban and Periurban Hedgerows
    - The hedgerow detection models will capture vegetation and height, which should work well near fields and open terrain, but will be more difficult in urban areas.
    - I'd expect to see hedgerows around a football pitch, but not along a terraced street, and gardens are dense and probably won't contain "linear features".

- Tim Farron MP said ["[Britain's hedgerows] would stretch to the moon and back"](https://parliamentlive.tv/event/index/b7ab5c1c-dbc6-4a16-8b7a-a74f4493c92d?in=10:08:40)?!
    - In England we have 420Mm of hedges, the average distance to the moon is 385Mm, so there and back is a stretch without Scotland and Wales.
    - Following, he did say "over half a million miles of hedgerows", which I imagine was just mis-speaking/reading "m" meters as miles.


### Data Sources
| License | Source | Dataset | Version | Suitability | Methodology | Contact / Link | Format |
|---|---|---|---|---|---|---|---|
| Open | Sentinel | S2A MSI |  | Imagery | Satellite | https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-2 | Raster
| Restricted | NE | LiDAR | Collected 2016-21 + updates | Imagery | Aerial | https://www.data.gov.uk/dataset/f0db0249-f17b-4036-9e65-309148c97ce4/national-lidar-programme | Raster
| Restricted | Bluesky | Aerial Photography |  | Imagery | Aerial + Satellite |  | Raster
| Restricted | Bluesky | Tree Map | ? | ? | Similar to VOM - Tree Detection | https://bluesky-world.com/ntm/ | Point
| Open | EA | Vegetation Object Model | 2021 | poor | Raster, LiDAR post-processed to provide a canopy height model.  Excludes vegetation under 2.5m. | https://environment.data.gov.uk/dataset/ecae3bef-1e1d-4051-887b-9dc613c928ec |  Raster
| PSGA | OS | Master Map | Superceeded by NGD | little hedge data |  |  | Vector
| PSGA | OS | NGD |  | no hedge data |  |  | Vector
| Open | OSM | boundary:hedge |  | sparce data | crowd sourced |  | Vector
| Restricted | RPA | Hedge Managed | Superceeded by Control | fine | OS Field Boundaries + AES Agreements |  | Linear
| Restricted | RPA | Hedge Control |  | fine | Tidied RPA - Hedge Managed, eligible hedges only |  | Linear
| Restricted | RPA | Classified Hedges | WIP | best | AI image recognition model using Sentinel + NE - LiDAR + AES Agreements, verified against RPA - Hedge Control |  | Polygon
| Restricted | FR | NCEA: Trees Outside Woodland | V2  | fine | Bluesky National Tree Map plus classification of aerial photography.  Hedgerows over 20m long, under 4m wide, and under 3m tall included. 336,000 km of hedgerow in England. | Ben.Ditchburn@ncea.gov.uk, Freddie.Hunter@forestresearch.gov.uk | Polygon
| PSGA? | OS | Field Boundaries | WIP | ? | ? |  | Linear
| Restricted | UKCEH | Land Cover Map Plus: Hedgerows |  | ? | NE-LiDAR + UKCEH-LCM validated against "OS data" | [ceh-catalogue](https://catalogue.ceh.ac.uk/documents/d90a3733-2949-4dfa-8ac2-a88aef8699be) [ceh-description](https://www.ceh.ac.uk/press/high-tech-aerial-mapping-reveals-englands-hedgerow-landscape) | ? |
| Internal | elmo_geo | hedge | WIP |  | Joins hedges to parcels and assigns the parcel boundary as hedge. | Andrew.West@defra.gov.uk | Linear
| Internal | elmo_geo | sylvan | WIP | | Uses VOM - Tree Detection [pycrown](https://github.com/obisargoni/pycrown) algorithm | Obi.ThompsonSargoni@defra.gov.uk | Linear
| Internal | elmo_geo | boundaries | WIP |  | hedge plus other boundary features | Andrew.West@defra.gov.uk, Obi.ThompsonSargoni@defra.gov.uk | Linear

##### Nomenclature
- OS - Ordnance Survey
- PSGA - OS's [Public Sector Geospatial Agreement](https://www.ordnancesurvey.co.uk/customers/public-sector/public-sector-licensing)
- NGD - National Geographic Database
- OSM - OpenStreetMap
- RPA - Rural Payments Agency
- FR - Forestry Research
- NCEA - Defra's Natural Capital and Ecosystem Assessment
- NE - Natural England


### Total Hedgerows
Table of Hedgerow datasets and their total lengths.  (for England except "2007")

|      | Total Length (m) | Explanation
| :--- | ---------------: | :---
| 1984 |      572,670,000 | Managed hedgerow in 1984, [EIP 2023](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1133967/environmental-improvement-plan-2023.pdf), "peak historic hedgerow"
| 2007 |      477,000,000 | Managed hedgerows in 2007, [CS 2007](https://www.ceh.ac.uk/sites/default/files/Countryside%20Survey%202007%20UK%20Headline%20Messages_Part2.pdf) *in Great Britain*
| EFA  |      640,341,286 | EFA hedgerows
| Adj  |  **419,882,012** | EFA hedgerows adjusted (half if a hedge is adjacent to another parcel)
| OSM  |      102,408,028 | OSM hedgerows
| OSMp |       73,510,661 | OSM hedgerows within parcels
| 110% |      630,000,000 | A target of 110% of managed hedgerow in 1984 by 2050
| +75  |      494,000,000 | A target of +75e6m of hedgerow, defined using Adj, by 2050
| +45  |      464,000,000 | A intermediate target of +45e6m of hedgerow by 2037
| Adj 2023 |  575,100,528 | Recalculated using RPA - Hedge Control - 2023_12_18
| TOW 2023 |  336,000,000 | FR - NCEA: Trees Outside Woodland model's hedgerow features
| CEH 2024 |  390,000,000 | UKCEH - Land Cover Map Plus: Hedgerows, [press release](https://www.ceh.ac.uk/press/high-tech-aerial-mapping-reveals-englands-hedgerow-landscape) figure

This table was originally created Dec 2022


### Hedgerow Targets
|      | Target      | Year | Increase | Annual Increase |
| ---: | ----------: | ---: | -------: | --------------: |
|  Adj | 419,882,012 | 2022 |          |                 |
| 110% | 630,000,000 | 2050 |    50.0% |            1.8% |
|  +75 | 494,000,000 | 2050 |    17.9% |            0.6% |
|  +45 | 464,000,000 | 2037 |    10.7% |            0.7% |

Table of Targets for Hedgerow increases


### Relict Hedgerow
![Relict Hedgerow](https://www.teagasc.ie/media/website/news/daily/environment-photos/Relict-Hedgerows---too-valuable-for-rejuvenation.jpg)

[Teagasc on hedgerows](https://www.teagasc.ie/news--events/daily/environment/hedges-for-rejuvenation.php)


### Interested People
- Defra.ELM.DSMT - Andrew.West@defra.gov.uk
- Defra.ELM.GHD - Obi.ThompsonSargoni@defra.gov.uk
- RPA.DataMart - Andrew.Osborn@rpa.gov.uk
- RPA.Geospatial - [Brian.O'Toole@rpa.gov.uk](mailto:Brian.O'Toole@rpa.gov.uk)
- RPA.Hegdes - Yajnaseni.Palchowdhuri@rpa.gov.uk
- Defra.TreeTeam - Iain Dummett, Chris McGurk, Rory Lunny
- Protected Landscapes - Liz Bingham
