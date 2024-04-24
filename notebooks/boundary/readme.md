
|   |   |
|---|---|
| **Authors** | Andrew West, Obi Thompson Sargoni, [elmo-geo](https://github.com/Defra-Data-Science-Centre-of-Excellence/elmo-geo) |
| **Updated** | 2024-04-18 |
| **Objective** | Determine parcel boundary uses |

# Boundary Use
This project produces multiple datasets that detail what features intersect parcel boundaries. This information can be used to inform option edibility. Relevant ELM Options are those that include the management (potentially with a buffer strip) or creation of boundaries.

## Methodology
1. [Segment](../segment.py): first the boundaries of parcels are split into segments, these will be determined to be totally a combination of features.
2. [Sylvan](../sylvan/readme.md): next boundary segments are assigned woody feature classifications, be that hedgerow, woodland, or relict hedges.  This uses the prioritisation method.  If it's hedgerow the boundary segment is not woodland or relict hedge (hedge>wood>relict).
3. [Boundary Use](boundary_use.py): All boundary segments are assigned to each additional feature; [adjacency](adjacency.py), sylvan, [water](water.py), [wall](wall.py), and [hefer](hefer.py).
    1. Feature geometries are buffered 12m to attach to nearby segments.  Linear features are considered 2m wide for cross compliance.
    2. If the proportion of the segment inside the (buffered) feature type is above a threshold, then it is "assigned" that feature type.
    3. A segment can be used by multiple feature types, otherwise it's considered "available".  This means it's suitable for options like hedgerow creation. As part of assessing adjacency we differentiate between parcels managed by the same or different farm business.

## Data
| Category | Dataset | Link | Description |
|---|---|---|---|
| Segment | [elmo_geo-business_info](../business_info.py) |
| Segment | [elmo_geo-segment](../segment.py) |
| Sylvan | [elmo_geo-sylvan](../sylvan/readme.md) |
| Water | OS, NGD | ... | ... |
| Wall | OSM, Heritage Wall | ... | ... |
| HEFER | RPA, HEFER | ... | ... |

## Output
Note: [class] = hedgerow, woodland, relict_hedge, waterway, waterbody, ditch, wall, hefer

### elmo_geo-boundary_use  
Schema: `id_business:int, id_parcel:str, id_boundary:int, m:float, is_[class]:bool`

### elmo_geo-buffer_strips
Schema: `id_business:int, id_parcel:str, m_[class], ha_[class]_buf[2,4,8,12]`

### <TILE> Map

### Intermediate Outputs

> Table 1 - Parcel Adjacency. Outpath - 'sf_adj'  
> This table is produced by performing a distance base spatial join between the parcels dataset and itself, with a 12m distance threshold. This provides a lookup from each parcel to its nearby parcels.
> |id_parcel|id_business|id_parcel_adj|id_business_adj|
> |---|---|---|---|
> |   |   |   |   |

> Table 2 - Neighbouring Land Use. Outpath - 'sf_neighbour'  
> This table has the nearby geometries for each parcel.  These are geometries joined to parcels with a distance of 12m, they are unioned according to their land use.
> |id_parcel |    geometry_boundary | geometry_adj_diff_bus|  geometry_adj_same_bus|  geometry_water| geometry_ditch| geometry_wall|  geometry_hedge|
> |---|---|---|---|---|---|---|--|
> |   |   |   |   |   |   |   |  |

> Table 3 - Boundary Section Use. Outpath - 'sf_boundary'  
> This table is the output from the boundary splitting methods. Each row is a section of the parcel boundary with boolean tags indicating which features this section intersects. elg_adj_diff_bus and elg_adj_same_bus differentiate between sections that boarder parcels belonging to the same business or not.
> |id_parcel| geometry_boundary|  elg_adj_diff_bus    |elg_adj_same_bus   |elg_water| elg_ditch|  elg_wall|   elg_hedge|
> |---|---|---|---|---|---|---|---|
> |   |   |   |   |   |   |   |   |

> Table 4 - Uptake. Outpath - 'sf_update'  
> This table is the Table 3 with additional parcel level classifications joined in. These classifications indicate whether a parcel has joined a woodland scheme (from EVAST data) or is in a wetland or peat area. It also calculates the total area and boundary length of the parcel.
> |id_business|   id_parcel|  farm_type|  priority_habitat|   elg_adj_diff_bus|   elg_adj_same_bus|   elg_water|  elg_ditch   |elg_wall|  elg_hedge   |woodland   |peatland|  wetland|    ha| m|
> |---|---|---|---|---|---|---|---|--|--|--|--|--|--|--|
> |   |   |   |   |   |   |   |   |  |  |  |  |  |  |  |


### Final Outputs

> Table 5 - Boundary Lengths. Outpath - 'sf_boundary_lengths'  
> This table sums the length of sections of parcel boundaries to produce total lengths of boundary sections per parcel. These lengths are used to inform parcel eligibility for ELMS actions.
> |id_parcel|m_boundary_unadj|m_boundary |m_water|   m_ditch|    m_wall| m_hedge|    m_available|m_available_same_business|  m_hedge_only|   m_hedge_on_ewco|    m_ditch_on_peatland|
> |---|---|---|---|---|---|---|---|---|---|---|---|
> |   |   |   |   |   |   |   |   |   |   |   |   |

> Table 6 - Total Feature Lengths.
> This table sums the lengths of source dataset features if they are linear geometries.  Polygons will not be considered.
> |class|m_england|m_inside_parcels|m_segments|
> |---|---|---|---|
> |hedgerow|   |   |   |
> |woodland|   |   |   |
> |relict hedgerow|   |   |   |
> |waterway|   |   |   |
> |waterbody|n/a|n/a|n/a|
> |ditch|   |   |   |
> |wall|   |   |   |
> |hefer|   |   |   |
> |available|   |   |   |



[references]: .
[townsend_water_buffer]: https://townsendcharteredsurveyors.co.uk/sustainable-farming-incentive-pilot-starting-2021-water-body-buffering-standard/
[hedgelink]: https://hedgelink.org.uk/
[Countryside Survey pt1]: https://www.ceh.ac.uk/sites/default/files/Countryside%20Survey%202007%20UK%20Headline%20Messages_Part1.pdf
[Countryside Survey pt2]: https://www.ceh.ac.uk/sites/default/files/Countryside%20Survey%202007%20UK%20Headline%20Messages_Part2.pdf
[Environmental Improvement Plan pg44-4]: https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1133967/environmental-improvement-plan-2023.pdf

<!--
# TODO
- Add adjacency
- Consider splitting up the boundary for each land use
- Consider recording if the boundary is adjacent to another (and such land use will be shared)
- Woodland Uptake, EVAST
- Priority Habitats, linked to parcels by 'process_dataset' notebook
- Peatland, linked to parcels by 'process_dataset' notebook
- Wetland, linked to parcels by 'process_dataset' notebook
- Urban

# Segment Data
| Category | Dataset | Link | Description |
|---|---|---|---|
| Boundary | RPA, Reference Parcels | ... | ... |
| Boundary | RPA, Land Cover | ... | ... |

# Sylvan Data
| Category | Dataset | Link | Description |
|---|---|---|---|
| Hedgerow | RPA, EFA Control Layer (Hedges) | ... | ... |
| Hedgerow | OSM, Hedgerows | ... | ... |
| Hedgerow, Woodland | EA, LiDAR VOM | ... | ... |
| Hedgerow, Woodland | FR, Trees Outside Woodland | ... | trees canopy and crown, linear/group/single | 
| Waterbody, Woodland | OS, NGD | ... | [https://osdatahub.os.uk/downloads/packages/2010]()

# OSM Tags
[tag finder](https://tagfinder.osm.ch/)
Hedgerows = barrier:[hedge,hedge_bank],landcover:hedge
Water = water:true,waterway:true
Wall = wall:dry_stone

# Tips
Use `ST_ReducePrecision(g, 0.001)` (1mm grid) - [gis#50399](https://gis.stackexchange.com/q/50399)  
Use `ST_Buffer(ST_Buffer(g, 0.001), buf)` (1mm pre-buffer) - [jts#876](https://github.com/locationtech/jts/issues/876)  

# Status
✔️⚙️❌
-->
