# Boundaries


## Data
### Source
- ods/rpa-parcel-adas
- stg/wfm-farm
- stg/wfm-field

- rpa-land_cover

- rpa-hedge_managed-2021
- rpa-hedge_control-2023
- os-ngd
- osm-uk



### Intermediate
- out/elmo-business_info
- ods/elmo_geo-land_cover
- ods/elmo_geo-segemented_boundary
- ods/elmo_geo-hedge
- ods/elmo_geo-water
- ods/elmo_geo-wall



### Output Data
- elmo_geo-segmented_boundary: id_parcel, id_boundary, geometry
  - methodology: parcel boundary + land cover boundaries within the parcel
- elmo_geo-hedge: id_parcel, source, class, geometry, width, height
  - classes: hedge
  - methodology: os-field_boundary/ngd + rpa-hedge_managed/control + osm-hedge + elmo_geo-sylvan
- elmo_geo-water: id_parcel, source, class, geometry
  - classes: watercourse, waterbody, trees_in_water, other
  - methodology: os-ngd + osm-water
- elmo_geo-wall: id_parcel, source, class, geometry
  - classes: wall, ruin, other
  - methodology: osm-wall + he-shine



### Output
- out/elmo-boundary
## Methodology


## QA Checks
### Plots



'''
00_readme should be the title page, merging the readmes
01_ingest is copied from aw-notebooks
02_splitting_method snapping to a segmented boundary
03_metrics should contain the output table
move Business Info to another place
load_sdf
delete copied bits from aw-notebooks once merged
QA
    ensure buffer totals are less than parcel total area
    ensure meter total is reasonable
    boundary segmentation method
    review my OSM and OS-NGD filters


'''
