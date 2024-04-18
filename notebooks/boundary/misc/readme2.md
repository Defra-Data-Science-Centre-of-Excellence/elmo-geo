# Hedgerows
This notebooks defines the hedgerow lengths and buffer areas to be added to the ELM population simulation.
Here I join hedgerows to parcels and businesses, clip to bounds, and calculate the length, and buffers.

EFA Hedge attaches the original OS data to parcels, but splits boundary hedges to each parcel, recording the as **adjacent**.
This means adjacent parcels are double counted.
I adjust for this as effectively half the hedgerow length.

Buffers are calculated for each hedgerow at [4, 6, 8, 10, 12] meters and clipped to the parcel, and unclipped buffer is also provided for the largest buffer.


### Data
- ELM Parcels `dbfs:/mnt/lab/unrestricted/elm/buffer_strips/parcels.parquet`
- EFA Hedge `dbfs:/mnt/lab/unrestricted/elm_data/rpa/efa_control/2023_02_07.parquet`
- OSM Hedgerows `dbfs:/mnt/lab/unrestricted/elm_data/osm/hedgerows.parquet`
- **Output:** ELM Hedgerows `dbfs:/mnt/lab/unrestricted/elm/buffer_strips/hedgerows.parquet`


### Table
> |      | Total Length (m) | Explanation
> | :--- | ---------------: | :---
> | 1984 |      572,670,000 | Managed hedgerow in 1984, [EIP 2023](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1133967/environmental-improvement-plan-2023.pdf)
> | 2007 |      477,000,000 | Managed hedgerows in 2007, [CS 2007](https://www.ceh.ac.uk/sites/default/files/Countryside%20Survey%202007%20UK%20Headline%20Messages_Part2.pdf) *in Great Britain*
> | EFA  |      640,341,286 | EFA hedgerows
> | Adj  |  **419,882,012** | EFA hedgerows adjusted for adjacent hedges
> | Elg  |      220,459,274 | EFA hedgerows adjusted for both adjacency and EFA eligibility
> | OSM  |      102,408,028 | OSM hedgerows
> | OSMp |       73,510,661 | OSM hedgerows within parcels
> | 110% |      630,000,000 | A target of 110% of managed hedgerow in 1984 by 2050
> |  +75 |      494,000,000 | A target of +75e6m of hedgerow, defined using Adj, by 2050
> |  +45 |      464,000,000 | A intermediate target of +45e6m of hedgerow by 2037
>
> Table of Hedgerow datasets and their total lengths.  (for England except "2007")

##### EFA Adjacency
"Adj" is the most appropriate measurement for total amount of hedgerow in England.
This data is sourced from RPA's EFA Hedge, which is derived from OS data, along with some additions from ES.
The alternative source OSM has much less data, and doesn't require further consideration.

##### EFA Eligibility
The sources do not come with a managed status for the hedgerows, and such we cannot tell whether or not they are relict.
EFA eligibility is not a good analogous, as it was determined from length and adjacent arable land cover.
However EFA calculations for BPS stopped in 2018.

##### Targets
> |      | Target      | Year | Increase | Annual Increase |
> | ---: | ----------: | ---: | -------: | --------------: |
> |  Adj | 419,882,012 | 2022 |          |                 |
> | 110% | 630,000,000 | 2050 |    50.0% |            1.8% |
> |  +75 | 494,000,000 | 2050 |    17.9% |            0.6% |
> |  +45 | 464,000,000 | 2037 |    10.7% |            0.7% |
>
> Table of Targets for Hedgerow increases


##### Managed Hedgerow
The managed status of all datasets' hedgerows is uncertain.

> ![Relict Hedgerow](https://www.teagasc.ie/media/website/news/daily/environment-photos/Relict-Hedgerows---too-valuable-for-rejuvenation.jpg)
>
> [Teagasc on hedgerows](https://www.teagasc.ie/news--events/daily/environment/hedges-for-rejuvenation.php)





##### Links
[Hedgerow Standard](https://townsendcharteredsurveyors.co.uk/sustainable-farming-incentive-pilot-starting-2021-hedgerows-standard/)
[Waterbody Standard](https://townsendcharteredsurveyors.co.uk/sustainable-farming-incentive-pilot-starting-2021-water-body-buffering-standard/)
[Sharepoint Document](https://defra.sharepoint.com/:w:/r/teams/Team1645/Evidence%20and%20Analysis%20WS/4.9_Workstream_Areas_Modelling_Strategy/4.9.3.5_Projects_MS5_Payment_rates/Buffering%20Payment%20Strategy%20Analysis.docx?d=w078dc8ee5888496c924a3e84be78ff55&csf=1&web=1&e=RHbHab)



loc = 'id_parcel REGEXP "NY9(1|2)7(0|1).*"'  # Humshaugh



##### OSM
`gdf = oxmnx.geometries_from_place(place, tags).reset_index()[["osmid", *tags.keys(), "geometry"]]`
