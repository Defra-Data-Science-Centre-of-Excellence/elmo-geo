# Databricks notebook source
# MAGIC %md
# MAGIC # Boundary
# MAGIC
# MAGIC ### Input Data
# MAGIC - wfm-field
# MAGIC - rpa-parcel
# MAGIC - rpa-hedge_managed-2021
# MAGIC - rpa-hedge_control-2023
# MAGIC - os-ngd
# MAGIC - osm-uk
# MAGIC
# MAGIC
# MAGIC ### Output Data
# MAGIC - elmo_geo-boundary: id_parcel, geometry
# MAGIC   - methodology: parcel boundary + land cover boundaries within the parcel
# MAGIC - elmo_geo-merged_hedge: id_parcel, source, class, geometry, width, height
# MAGIC   - classes: hedge
# MAGIC   - methodology: os-field_boundary/ngd + rpa-hedge_managed/control + osm-hedge + elmo_geo-sylvan
# MAGIC - elmo_geo-merged_water: id_parcel, source, class, geometry
# MAGIC   - classes: watercourse, waterbody, trees_in_water, other
# MAGIC   - methodology: os-ngd + osm-water
# MAGIC - elmo_geo-merged_wall: id_parcel, source, class, geometry
# MAGIC   - classes: wall, ruin, other
# MAGIC   - methodology: osm-wall + he-shine

# COMMAND ----------

# MAGIC %md # notes
# MAGIC update buffer method - https://defra.sharepoint.com/:w:/r/teams/Team1645/_layouts/15/Doc.aspx?sourcedoc=%7B078DC8EE-5888-496C-924A-3E84BE78FF55%7D&file=Buffering%20Payment%20Strategy%20Analysis.docx&action=default&mobileredirect=true
# MAGIC
# MAGIC # GIS projects
# MAGIC [ ] update hedge doc
# MAGIC [ ] waterbodies
# MAGIC [ ] other boundaries (walls)
# MAGIC [ ] woodland - hedgerow is not woodland, commercial is not woodland
# MAGIC [ ] grassland
# MAGIC [ ] moorland
# MAGIC [ ] commons
# MAGIC [ ] tenants
# MAGIC [ ] priority habitat distance
# MAGIC
# MAGIC
# MAGIC 	boundary features
# MAGIC 	  sylvan
# MAGIC 	    hedge  (rpa-cl_hedge)
# MAGIC 	    wood  (?)
# MAGIC 	    relict  (fr-tow)
# MAGIC 	  water  (os-ngd-wtr*)
# MAGIC 	  wall (osm-wall, os-ngd-?, ne-shine, he-hefer)
# MAGIC 	proportional features  (geoportals for all these)
# MAGIC 	  moor
# MAGIC 	  lfa
# MAGIC 	  sssi
# MAGIC 	  np
# MAGIC 	  nl (aonb)
# MAGIC 	  ...
# MAGIC
# MAGIC
# MAGIC
# MAGIC 	Project: Run
# MAGIC 	Data Sources
# MAGIC 		None
# MAGIC 	Methodology
# MAGIC 		$run_notebook
# MAGIC 	Output
# MAGIC 		elmo_geo-run_success
# MAGIC
# MAGIC
# MAGIC 	Project: Parcels
# MAGIC 	Data Sources
# MAGIC 		rpa-ref_parcel
# MAGIC 		wfm-farms
# MAGIC 		wfm-fields
# MAGIC 	Methodology
# MAGIC 		join
# MAGIC 	Output
# MAGIC 		elmo_geo-parcels: id_parcel, 
# MAGIC
# MAGIC
# MAGIC 	Project: Features
# MAGIC 	Data Sources
# MAGIC 		elmo_geo-parcels
# MAGIC 		ons-country
# MAGIC 		ons-itl1_region
# MAGIC 		ons-ita3_cua
# MAGIC 		ons-parish
# MAGIC 		ons/ne-national_park
# MAGIC 		ne-sssi_units
# MAGIC 		ne-aonb
# MAGIC 		ne-commons
# MAGIC 		ne-priority_habitat
# MAGIC 		ne-moorline
# MAGIC 		ne-...
# MAGIC 	Methodology
# MAGIC 		sjoin
# MAGIC 		sorted hstack
# MAGIC 	Output
# MAGIC 		elmo_geo-features: id_parcel, id_business, id_sub_business, farm_type, arable_or_grassland, aes, region, p_region, ...
# MAGIC
# MAGIC
# MAGIC 	Project: Sylvan
# MAGIC 	Data Sources
# MAGIC 		obi
# MAGIC 	Methodology
# MAGIC 		obi
# MAGIC 	Output
# MAGIC 		elmo_geo-sylvan: key, id_parcel, sylvan_type, geometry
# MAGIC
# MAGIC
# MAGIC 	Project: Water
# MAGIC 	Data Sources
# MAGIC 		os-wtr
# MAGIC 		osm-water
# MAGIC 	Methodology
# MAGIC 		wrangle
# MAGIC 		concat
# MAGIC 		sjoin 12m buf
# MAGIC 	Output
# MAGIC 		elmo_geo-water: id_parcel, data_source, water_type, geometry
# MAGIC
# MAGIC
# MAGIC 	Project: Wall
# MAGIC 	Data Sources
# MAGIC 		os-lnd *tag=wall
# MAGIC 		osm-wall
# MAGIC 		rpa-agreements (wall)
# MAGIC 	Methodology
# MAGIC 		wrangle
# MAGIC 		concat
# MAGIC 		sjoin 12m buf
# MAGIC 	Output
# MAGIC
# MAGIC
# MAGIC 	Project: Boundaries
# MAGIC 	Data Sources
# MAGIC 		rpa-ref_parcels
# MAGIC 		elmo_geo-sylvan
# MAGIC 		elmo_geo-water
# MAGIC 		elmo_geo-wall
# MAGIC 	Methodology
# MAGIC 		splitting method
# MAGIC 	Output
# MAGIC 		elmo_geo-boundaries: id_boundary, id_parcel, id_business, b_hedge, b_woodland, b_rhedge, b_water, b_ditch, b_wall,
# MAGIC

# COMMAND ----------

import numpy as np
import pandas as pd
import geopandas as gpd
from pyspark.sql import functions as F, types as T

import elmo_geo
from elmo_geo import LOG
from elmo_geo.io import load_sdf, to_gpq, download_link
from elmo_geo.io.file import st_simplify
from elmo_geo.st import sjoin
from elmo_geo.st.udf import st_union

elmo_geo.register()

def log_info(sdf):
    LOG.info(f'''
        Count: {sdf.count():,}
        Partitions: {sdf.rdd.getNumPartitions():,}
        Columns: {sdf.columns}
    ''')
    return sdf

def cache(sdf):
    sdf.write.format('noop').save()
    return log_info(sdf)


# COMMAND ----------

# Business Info
ha_arable = ['ha_fallow','ha_field_beans','ha_fodder_maize','ha_grain_maize','ha_other_crop','ha_peas','ha_potatoes','ha_oilseed_rape','ha_spring_barley','ha_spring_oats','ha_spring_wheat','ha_winter_barley','ha_winter_oats','ha_winter_wheat']
ha_grassland = ['ha_disadvantaged','ha_fenland','ha_lowland_other','ha_improved_disadvantaged','ha_improved_grades_1_2','ha_improved_grades_3_4_5','ha_moorland','ha_severely_disadvantaged','ha_temporary_pasture','ha_unimproved','ha_unimproved_disadvantaged']
ha_lowland = ['ha_fallow','ha_fenland','ha_field_beans','ha_fodder_maize','ha_grain_maize','ha_lowland_other','ha_improved_grades_1_2','ha_improved_grades_3_4_5','ha_other_crop','ha_peas','ha_potatoes','ha_oilseed_rape','ha_spring_barley','ha_spring_oats','ha_spring_wheat','ha_sugar_beet','ha_temporary_pasture','ha_unimproved','ha_winter_barley','ha_winter_oats','ha_winter_wheat']
ha_upland = ['ha_disadvantaged','ha_improved_disadvantaged','ha_moorland','ha_severely_disadvantaged','ha_unimproved_disadvantaged']


sdf_wfm_farm = pd.read_feather(
    '/dbfs/mnt/lab/restricted/ELM-Project/stg/wfm-farm-2024_01_26.feather',
    columns = ['id_business', 'farm_type']
).pipe(spark.createDataFrame)

sdf_wfm_field = pd.read_feather(
    '/dbfs/mnt/lab/restricted/ELM-Project/stg/wfm-field-2024_01_26.feather',
    columns = set(['id_business', 'id_parcel', *ha_arable, *ha_grassland, *ha_lowland, *ha_upland])
).pipe(spark.createDataFrame)

# sdf_wfm_region = (
#     pd.read_feather('/dbfs/mnt/lab/unrestricted/elm/elmo/region/region.feather')
#     .sort_values('proportion')
#     .groupby('id_parcel').first().reset_index()
#     .pipe(spark.createDataFrame)
# )


sdf_wfm = (sdf_wfm_field
    .selectExpr(
        'id_business', 'id_parcel',
        f'{"+".join(ha_arable)} AS ha_arable',
        f'{"+".join(ha_grassland)} AS ha_grassland',
        f'{"+".join(ha_lowland)} AS ha_lowland',
        f'{"+".join(ha_upland)} AS ha_upland',
    )
    .groupby('id_business').agg(
        F.expr('COLLECT_SET(id_parcel) AS id_parcel'),
        F.expr('CASE WHEN sum(ha_arable) < sum(ha_grassland) THEN "grassland" ELSE "arable" END AS arable_grassland'),
        F.expr('CASE WHEN sum(ha_lowland) < sum(ha_upland) THEN "upland" ELSE "lowland" END AS lowland_upland'),
    )
    .join(sdf_wfm_farm, on='id_business', how='outer')
    # .withColumn('id_parcel', F.explode('id_parcel'))
    # .join(sdf_wfm_region, on='id_parcel', how='outer')
    .selectExpr(
        'id_business', 'id_parcel',
        'LOWER(farm_type) AS farm_type',
        'arable_grassland', 'lowland_upland',
        # 'region', 'proportion AS p_region',
    )
)



sdf_wfm.display()
sdf_wfm.coalesce(1).write.parquet('dbfs:/mnt/lab/restricted/ELM-Project/out/elmo_geo-business_info-2024_01_30.parquet')

# COMMAND ----------


# Boundary
sdf_rpa_parcel = load_sdf('rpa-parcel-adas').select('id_parcel', 'geometry')


sdf_land_cover = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-land_cover_lookup-2023_11_07.parquet')
    .join(
        (spark.read.format('parquet')
            .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-land_cover-2023_12_13.parquet')
            .selectExpr('LAND_COVER_CLASS_CODE', 'ST_SetSRID(ST_GeomFromWKB(GEOM), 27700) AS geometry')
            .repartition(200)
        ),
        on = 'LAND_COVER_CLASS_CODE',
        how = 'outer',
    )
    .withColumn('geometry', st_simplify())
    .withColumn('geometry', F.expr('EXPLODE(ST_Dump(geometry))'))
    .transform(lambda sdf: sjoin(sdf_rpa_parcel, sdf, rsuffix=''))
    .drop('geometry_left')
    .transform(to_gpq, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-land_cover-2024_01_26.parquet')
)

sdf_boundary = (load_sdf('elmo_geo-land_cover')
    .select('id_parcel', 'geometry')
    .withColumn('geometry', st_simplify())
    .transform(st_union, 'id_parcel', 'geometry')
    .join(
        sdf_rpa_parcel.selectExpr('id_parcel', 'geometry AS geometry_parcel'),
        on='id_parcel',
        how='outer',
    )
    .selectExpr(
        'id_parcel',
        '''ST_CollectionExtract(ST_Collect(
            ST_Boundary(geometry_parcel),
            ST_Intersection(ST_Boundary(geometry), geometry_parcel)
        ), 2) AS geometry''',  # bounaries around and inside, drop points
    )
    .transform(to_gpq, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-boundary-2024_02_01.parquet')
)


display(sdf_land_cover)
display(sdf_boundary)

# COMMAND ----------

# Hedge
sdf_rpa_parcel = load_sdf('rpa-parcel-adas').select('id_parcel', 'geometry')


sdf_os_ngd_hedge = (spark.read.format('parquet')
    .schema('layer string, theme string, description string, width double, geometry binary')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/os-ngd-2022.parquet')
    .filter('description == "Hedge"')
    .selectExpr(
        '"os-ngd-2022" AS source',
        # 'layer', 'theme', 'description',
        'ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry',
    )
)

sdf_os_boundary_hedge = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/os-field_boundary-sample_v1_2.parquet')
    .filter('description == "Hedge"')
    .selectExpr(
        '"os-field_boundary-sample_v1_2" AS source',
        # '"field_boundary" AS layer', 'theme', 'description',
        'ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry',
        # 'averagewidth_m AS m_width', 'averageheight_m AS m_height',
    )
)

sdf_rpa_hedge_managed = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-hedge-adas.parquet')
    .selectExpr(
        '"rpa-hedge_managed-adas" AS source',
        # F.expr('TO_TIMESTAMP(VALID_FROM, "yyyyMMddHHmmss") AS valid_from'),
        # F.expr('TO_TIMESTAMP(VALID_TO, "yyyyMMddHHmmss") AS valid_to'),
        'ST_SetSRID(ST_GeomFromWKB(GEOM), 27700) AS geometry',
    )
)

sdf_rpa_hedge_control = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-hedge-2023_12_13.parquet')
    .selectExpr(
        '"rpa-hedge_control-2023_12_13" AS source',
        # 'CONCAT(REF_PARCEL_SHEET_ID, REF_PARCEL_PARCEL_ID) AS id_parcel',
        # 'ADJACENT_PARCEL_PARCEL_ID IS NOT NULL AS adj',
        # 'TO_TIMESTAMP(VALID_FROM, "yyyyMMddHHmmss") AS valid_from',
        # 'TO_TIMESTAMP(VALID_TO, "yyyyMMddHHmmss") AS valid_to',
        'ST_SetSRID(ST_GeomFromWKB(GEOM), 27700) AS geometry',
    )
)

sdf_osm_hedge = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/osm-britain_and_ireland-2023_10_12.parquet')
    .selectExpr(
        '"osm-britain_and_ireland-2023_12_13" AS source',
        '''CONCAT(
            "highway=>", NVL(highway, ""),
            ",waterway=>", NVL(waterway, ""),
            ",aerialway=>", NVL(aerialway, ""),
            ",barrier=>", NVL(barrier, ""),
            ",man_made=>", NVL(man_made, ""),
            ",railway=>", NVL(railway, ""),
            ",", NVL(other_tags, "")
        ) AS tags''',
        'ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry',
    )
    .filter('tags LIKE "%hedge%"')
    .drop('tags')
)


sdf_hedge = (sdf_os_ngd_hedge
    .union(sdf_os_boundary_hedge)
    .union(sdf_rpa_hedge_managed)
    .union(sdf_rpa_hedge_control)
    .union(sdf_osm_hedge)
    .withColumn('class', F.lit('hedge'))
    .withColumn('geometry', st_simplify())
    .withColumn('geometry', F.expr('ST_SubDivideExplode(geometry, 256)'))
    .transform(lambda sdf: sjoin(sdf_rpa_parcel, sdf, rsuffix='', distance=12))
    .transform(st_union, ['source', 'id_parcel', 'class'], 'geometry')
    .transform(to_gpq, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-hedge-2024_01_26.parquet')
)


sdf_hedge.groupby('source').count().display()
display(sdf_hedge)

# COMMAND ----------

# Water
sdf_rpa_parcel = load_sdf('rpa-parcel-adas').select('id_parcel', 'geometry')


sdf_os_water = (spark.read.format('parquet')
    .schema('layer string, theme string, description string, width double, geometry binary')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/os-ngd-2022.parquet')
    .repartition(200)
    .join(
        spark.read.parquet('dbfs:/mnt/lab/restricted/ELM-Project/stg/awest-os_water_lookup-2024_01_26.parquet'),
        on = 'description',
        how = 'inner',
    )
    .selectExpr(
        '"os-ngd-2022" AS source',
        'CONCAT("water-", watertype) AS class',
        'ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry',
    )
)

sdf_osm_water = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/osm-britain_and_ireland-2023_10_12.parquet')
    .repartition(200)
    .selectExpr(
        '"osm-britain_and_ireland-2023_12_13" AS source',
        '''SUBSTRING(CONCAT(
            NVL(CONCAT(",highway=>", highway), ""),
            NVL(CONCAT(",waterway=>", waterway), ""),
            NVL(CONCAT(",aerialway=>", aerialway), ""),
            NVL(CONCAT(",barrier=>", barrier), ""),
            NVL(CONCAT(",man_made=>", man_made), ""),
            NVL(CONCAT(",railway=>", railway), ""),
            NVL(CONCAT(",", other_tags), "")
        ), 2) AS tags''',
        'ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry',
    )
    .filter('tags LIKE \'%"water%"=>%\'')
    .selectExpr(
        'source',
        '''CASE
            WHEN (tags LIKE '%drain%' OR tags LIKE '%ditch%') THEN "water-ditch"
            WHEN (tags LIKE '%"%waterway%"=>%') THEN "water-watercourse"
            ELSE "water-waterbody"
        END AS class''',
        'geometry',
    )
)


sdf_water = (sdf_os_water
    .union(sdf_osm_water)
    .withColumn('geometry', st_simplify())
    .withColumn('geometry', F.expr('ST_SubDivideExplode(geometry, 256)'))
    .transform(lambda sdf: sjoin(sdf_rpa_parcel, sdf, rsuffix='', distance=12))
    .transform(st_union, ['source', 'id_parcel', 'class'], 'geometry')
    .transform(to_gpq, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-water-2024_01_26.parquet')
)


display(sdf_water)

# COMMAND ----------

# Wall
sdf_rpa_parcel = load_sdf('rpa-parcel-adas').select('id_parcel', 'geometry')

sdf_os_ngd_wall = (spark.read.format('parquet')
    .schema('layer string, theme string, description string, width double, geometry binary')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/os-ngd-2022.parquet')
    .filter('description LIKE "%Wall%"')
    .selectExpr(
        '"os-ngd-2022" AS source',
        # 'layer', 'theme', 'description',
        '''CASE
            WHEN LOWER(description) LIKE "%wall%" THEN "wall-wall"
            ELSE "wall-other"
        END AS class''',
        'ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry',
    )
)

sdf_os_boundary_wall = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/os-field_boundary-sample_v1_2.parquet')
    .filter('description == "Wall"')
    .selectExpr(
        '"os-field_boundary-sample_v1_2" AS source',
        # '"field_boundary" AS layer', 'theme', 'description',
        '''CASE
            WHEN LOWER(description) LIKE "%wall%" THEN "wall-wall"
            ELSE "wall-other"
        END AS class''',
        'ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry',
        # 'averagewidth_m AS m_width', 'averageheight_m AS m_height',
    )
)

sdf_osm_wall = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/osm-britain_and_ireland-2023_10_12.parquet')
    .selectExpr(
        '"osm-britain_and_ireland-2023_12_13" AS source',
        'barrier', 'other_tags',
        'ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry',
    )
    .filter('barrier IS NOT null OR other_tags LIKE \'%barrier"=>%\' OR other_tags LIKE \'%wall"=>%\'')
    .selectExpr(
        'source',
        '''CASE
            WHEN LOWER(barrier) LIKE "%wall%" THEN "wall-wall"
            ELSE "wall-other"
        END AS class''',
        'geometry',
    )
)

sdf_he_shine = (spark.read.format('parquet')
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/he-shine-2022_12_30.parquet')
    .selectExpr(
        '"he-shine-2022_12_30" AS source',
        '''CASE
            WHEN shine_form != " " THEN "wall-relict"
            ELSE "wall-other"
        END AS class''',
        'ST_SetSRID(ST_GeomFromWKB(geom), 27700) AS geometry',
    )
)

sdf_wall = (sdf_os_ngd_wall
    .union(sdf_os_boundary_wall)
    .union(sdf_osm_wall)
    .union(sdf_he_shine)
    .withColumn('geometry', st_simplify())
    .withColumn('geometry', F.expr('EXPLODE(ST_Dump(geometry))'))
    .transform(lambda sdf: sjoin(sdf_rpa_parcel, sdf, rsuffix='', distance=12))
    .transform(st_union, ['source', 'id_parcel', 'class'], 'geometry')
    .transform(to_gpq, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/elmo_geo-wall-2024_01_26.parquet')
)


display(sdf_wall)

# COMMAND ----------

#boundary splitting method...
sdf_info = spark.read.parquet('dbfs:/mnt/lab/restricted/ELM-Project/out/elmo_geo-business_info-2024_01_30.parquet')
sdf_boundary = load_sdf('elmo_geo-boundary')

sdf_hedge = load_sdf('elmo_geo-hedge')
sdf_water = load_sdf('elmo_geo-water')
sdf_wall = load_sdf('elmo_geo-wall')
