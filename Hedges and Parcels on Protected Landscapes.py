# Databricks notebook source
# MAGIC %md
# MAGIC # Hedges and Parcels on Protected Landscapes
# MAGIC **Protected Landscapes,** (PL) are both National Parks (NP) and National Landscapes (NL) (previously known as Areas of Outstanding National Beauty, AONB).  An additional unified PL geometry is appeneded (in v2023_12_15).  
# MAGIC **Hedges,** are spatially joined to PL using ST_Intersects, but not clipped to the PL.  After joining, they are groupby PL Type + PL Name, I then calculate the area for PL and the length for all hedgerows in that PL.  
# MAGIC **Parcels,** are spatially joined to PL using the same method.  No groupby is done, but I drop the geometries only keeping unique id_parcels on each PL and the proportion of that parcel's overlap with that PL, so near zero overlaps can be filtered later.  Latest supply from RPA is used (in v2023_12_15).  
# MAGIC
# MAGIC ### Data
# MAGIC - ne-national_parks-2023_11_17
# MAGIC - ne-aonb-2020_08_25
# MAGIC - rpa-parcel-2023_12_13
# MAGIC - rpa-hedge-2023_12_13
# MAGIC - elmo_geo-protected-landscapes-2023_12_15
# MAGIC
# MAGIC ### Output
# MAGIC - [awest-hedges_on_pl-2023_12_15.feather](https://adb-7422054397937474.14.azuredatabricks.net/files/awest-hedges_on_pl-2023_12_15.feather?o=7422054397937474)
# MAGIC - [awest-parcels_on_pl-2023_12_15.feather](https://adb-7422054397937474.14.azuredatabricks.net/files/awest-parcels_on_pl-2023_12_15.feather?o=7422054397937474)

# COMMAND ----------

import pandas as pd
import geopandas as gpd
from pyspark.sql import functions as F

import elmo_geo
from elmo_geo.io import load_sdf, to_gpq, download_link
from elmo_geo.st import sjoin

elmo_geo.register()

# COMMAND ----------

# Protected Landscapes (PL)
gdf_np = gpd.read_parquet('/dbfs/mnt/lab/restricted/ELM-Project/stg/ne-national_parks-2023_11_17.parquet')
gdf_np['PL Type'] = 'National Park'
gdf_np['PL Name'] = gdf_np['NAME']
gdf_np = gdf_np[['PL Type', 'PL Name', 'geometry']]

gdf_nl = gpd.read_parquet('/dbfs/mnt/lab/restricted/ELM-Project/stg/ne-aonb-2020_08_25.parquet').to_crs(epsg=27700)
gdf_nl['PL Type'] = 'National Landscape'
gdf_nl['PL Name'] = gdf_nl['NAME']
gdf_nl = gdf_nl[['PL Type', 'PL Name', 'geometry']]


gdf_pl = pd.concat([gdf_np, gdf_nl])
gdf_pl = pd.concat([gdf_pl, gpd.GeoDataFrame({
    'PL Name': ['All Protected Landscapes'],
    'geometry': [g],
}, crs=gdf_pl.crs)])
gdf_pl.geometry = gdf_pl.simplify(0.001)


gdf_pl.to_parquet('/dbfs/mnt/lab/restricted/ELM-Project/ods/elmo_geo-protected_landscapes-2023_12_15.parquet')

# COMMAND ----------

# Hedges on PL
f = '/dbfs/mnt/lab/restricted/ELM-Project/out/awest-hedges_on_pl-2023_12_15.feather'
sdf_pl = load_sdf('elmo_geo-protected_landscapes-2023_12_15')
sdf_hedge = load_sdf('rpa-hedge-2023_12_13')


sdf_pl.repartition(sdf_pl.count()).createOrReplaceTempView('pl')
sdf_hedge.createOrReplaceTempView('hedge')
sdf = spark.sql('''
    SELECT
        l.`PL Type`,
        l.`PL Name`,
        FIRST(l.ha) AS ha_pl,
        SUM(r.m) AS m_hedge
    FROM (
        SELECT
            ST_Length(geometry) AS m,
            geometry
        FROM hedge
    ) r
    JOIN (
        SELECT
            `PL Type`,
            `PL Name`,
            ST_Area(geometry)/10000 AS ha,
            ST_SubDivideExplode(ST_MakeValid(ST_SimplifyPreserveTopology(geometry, 1)), 256) AS geometry
        FROM pl
    ) l
    ON l.geometry IS NULL OR ST_Intersects(l.geometry, r.geometry)
    GROUP BY
        l.`PL Type`,
        l.`PL Name`
''')


sdf.toPandas().to_feather(f)
download_link(f)
display(sdf)

# COMMAND ----------

# Parcels on PL
f = '/dbfs/mnt/lab/restricted/ELM-Project/out/awest-parcels_on_pl-2023_12_15.feather'
sdf_pl = load_sdf('elmo_geo-protected_landscapes-2023_12_15')
sdf_parcel = load_sdf('rpa-parcel-2023_12_13')


sdf_pl.repartition(sdf_pl.count()).createOrReplaceTempView('pl')
sdf_parcel.createOrReplaceTempView('parcel')
sdf = spark.sql('''
    SELECT
        `PL Type`,
        `PL Name`,
        id_parcel,
        ST_Area(ST_MakeValid(ST_Intersection(geometry_parcel, geometry_pl))) / ST_Area(geometry_parcel) AS proportion
    FROM (
        SELECT
            l.`PL Type`,
            l.`PL Name`,
            r.id_parcel,
            ST_MakeValid(ST_SimplifyPreserveTopology(ST_MakeValid(ST_Union_Aggr(l.geometry)), 1)) AS geometry_pl,
            ST_MakeValid(ST_SimplifyPreserveTopology(ST_MakeValid(ST_Union_Aggr(r.geometry)), 0.1)) AS geometry_parcel
        FROM (
            SELECT
                id_parcel,
                geometry
            FROM parcel
        ) AS r
        JOIN (
            SELECT
                `PL Type`,
                `PL Name`,
                ST_SubDivideExplode(ST_MakeValid(ST_SimplifyPreserveTopology(geometry, 1)), 256) AS geometry
            FROM pl
        ) AS l
        ON ST_Intersects(l.geometry, r.geometry)
        GROUP BY
            l.`PL Type`,
            l.`PL Name`,
            r.id_parcel
    )
''')


sdf.toPandas().to_feather(f)
download_link(f)
display(sdf)
