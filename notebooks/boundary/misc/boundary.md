# Boundary

### Input Data
- wfm-field
- rpa-parcel
- rpa-hedge_managed-2021
- rpa-hedge_control-2023
- os-ngd
- osm-uk


# notes
update buffer method - https://defra.sharepoint.com/:w:/r/teams/Team1645/_layouts/15/Doc.aspx?sourcedoc=%7B078DC8EE-5888-496C-924A-3E84BE78FF55%7D&file=Buffering%20Payment%20Strategy%20Analysis.docx&action=default&mobileredirect=true

# GIS projects
[ ] update hedge doc
[ ] waterbodies
[ ] other boundaries (walls)
[ ] woodland - hedgerow is not woodland, commercial is not woodland
[ ] grassland
[ ] moorland
[ ] commons
[ ] tenants
[ ] priority habitat distance


	boundary features
	  sylvan
	    hedge  (rpa-cl_hedge)
	    wood  (?)
	    relict  (fr-tow)
	  water  (os-ngd-wtr*)
	  wall (osm-wall, os-ngd-?, ne-shine, he-hefer)
	proportional features  (geoportals for all these)
	  moor
	  lfa
	  sssi
	  np
	  nl (aonb)
	  ...



	Project: Run
	Data Sources
		None
	Methodology
		$run_notebook
	Output
		elmo_geo-run_success


	Project: Parcels
	Data Sources
		rpa-ref_parcel
		wfm-farms
		wfm-fields
	Methodology
		join
	Output
		elmo_geo-parcels: id_parcel, 


	Project: Features
	Data Sources
		elmo_geo-parcels
		ons-country
		ons-itl1_region
		ons-ita3_cua
		ons-parish
		ons/ne-national_park
		ne-sssi_units
		ne-aonb
		ne-commons
		ne-priority_habitat
		ne-moorline
		ne-...
	Methodology
		sjoin
		sorted hstack
	Output
		elmo_geo-features: id_parcel, id_business, id_sub_business, farm_type, arable_or_grassland, aes, region, p_region, ...


	Project: Sylvan
	Data Sources
		obi
	Methodology
		obi
	Output
		elmo_geo-sylvan: key, id_parcel, sylvan_type, geometry


	Project: Water
	Data Sources
		os-wtr
		osm-water
	Methodology
		wrangle
		concat
		sjoin 12m buf
	Output
		elmo_geo-water: id_parcel, data_source, water_type, geometry


	Project: Wall
	Data Sources
		os-lnd *tag=wall
		osm-wall
		rpa-agreements (wall)
	Methodology
		wrangle
		concat
		sjoin 12m buf
	Output


	Project: Boundaries
	Data Sources
		rpa-ref_parcels
		elmo_geo-sylvan
		elmo_geo-water
		elmo_geo-wall
	Methodology
		splitting method
	Output
		elmo_geo-boundaries: id_boundary, id_parcel, id_business, b_hedge, b_woodland, b_rhedge, b_water, b_ditch, b_wall,

