# Databricks notebook source
import os
from glob import glob
import pandas as pd
import geopandas as gpd
from pyspark.sql import functions as F, types as T

import elmo_geo
from elmo_geo.io import to_gpq, load_sdf, download_link
from elmo_geo.io.file import st_simplify
from elmo_geo.st import sjoin
from elmo_geo.st.udf import st_union

elmo_geo.register()

# COMMAND ----------

sdf = load_sdf('osm-britain_and_ireland')
sdf.display()
sdf.count()

# COMMAND ----------

# sdf.select('highway','waterway','aerialway','barrier','man_made','railway').distinct().display()
sdf.select('waterway').distinct().display()

# COMMAND ----------

# Inputs: df_wfm, df_so_table
# Out1:  df_so_parcel - id_business, id_parcel, so_<product>, p_lfa
# Out2:  df_so_business - id_business, so_<robust_farm_type>
# Out3:  df_farm_type - id_business, calculated_so_farm_type, calculated_gm_farm_type
# Out4:  df_diversity - id_business, diversity
import pandas as pd


# Data
df_wfm = pd.read_feather('/dbfs/mnt/lab/restricted/ELM-Project/stg/wfm-field-2023_11_28.feather')  # 2023_06_09/2023_11_28
# Groups
farm_types = 'cereal', 'horticulture', 'general_cropping', 'dairy', 'lfa_livestock', 'lowland_livestock', 'pigs', 'poultry'
lowland = 'improved', 'improved_grades_1_2', 'improved_grades_3_4_5', 'unimproved', 'other_lowland'
upland = 'disadvantaged', 'fenland', 'improved_disadvantaged', 'moorland', 'severely_disadvantaged', 'unimproved_disadvantaged',
dairy = 'dairy_beef_finishers_summer', 'dairy_beef_finishers_winter', 'dairy_beef_stores_summer', 'dairy_beef_stores_winter', 'dairy_cows', 'dairy_heifers',
livestock = 'beef_calves_autumn', 'beef_calves_spring', 'beef_finishers_summer', 'beef_finishers_winter', 'beef_heifers_older', 'goats', 'sheep',
fodder = 'fodder_maize', 'temporary_pasture',
horticulture = 'field_beans', 'other_crop', 'peas', 'potatoes', 'rapeseed_oil', 'sugar_beet',
cereal = 'spring_barley', 'spring_oats', 'spring_wheat', 'winter_barley', 'winter_oats', 'winter_wheat', 'grain_maize',


df_wfm

# COMMAND ----------

def group_produce_to_farm_type(k):
    '''Group WFM produce together to calculate contribution towards robust farm types.
    Likely k: Gross Margins, Standard Output, Standard Labour Requirements
    Requiring the columns: <gm/so/slr>_<produce>
    '''
    return {
        k+'_cereal': lambda df: df[[k+'_'+col for col in cereal]].sum(axis=1),
        k+'_horticulture': lambda df: df[[k+'_'+col for col in horticulture]].sum(axis=1),
        k+'_general_cropping': lambda df: df[k+'_cereal'] + df[k+'_horticulture'],
        k+'_dairy': lambda df: df[[k+'_'+col for col in dairy]].sum(axis=1),
        k+'_livestock': lambda df: df[[k+'_'+col for col in livestock+fodder]].sum(axis=1),
        k+'_lfa_livestock': lambda df: df[k+'_livestock'] * df['p_lfa'],
        k+'_lowland_livestock': lambda df: df[k+'_livestock'] * (1 - df['p_lfa']),
        k+'_pigs': None,
        k+'_poultry': None,
    }


df_so_business = (df_wfm
    .assign(
        p_lfa = lambda df: df[['ha_'+col for col in upland]].sum(axis=1) / df['ha_parcel_uaa'],
        **{'gm_'+col: lambda df: df['gmph_'+col] * df['ha_'+col] for col in cereal+horticulture+fodder},
        **{'gm_'+col: lambda df: df['gmpn_'+col] * df['n_'+col] for col in dairy+livestock},
        **group_produce_to_farm_type('gm')  # inputs gm_<produce>+p_lfa returns gm_<farm_type>
        # **group_produce_to_farm_type('so')  # inputs so_<produce>+p_lfa returns so_<farm_type>
    )
    .groupby('id_business')[['gm_'+col for col in farm_types]].sum(min_count=1)
)

df_so_business

# COMMAND ----------

def calc_farm_type(row):
    farm_type = 'mixed'
    if 2/3 * row.sum() < row.max():
        farm_type = '_'.join(row.idxmax().split('_')[1:])
    return farm_type


  df = df_so_business.copy()
df['farm_type'] = (df
    [['gm_'+col for col in farm_types]]
    .apply(calc_farm_type, axis=1)
)
df

# COMMAND ----------

def calc_cv(row):
    p_row = row / row.sum()
    return p_row.std() / p_row.mean()

df['cv'] = df[['so_'+col for col in farm_types]].apply(calc_cv, axis=1)

# COMMAND ----------

import pandas as pd
import geopandas as gpd
from pyspark.sql import functions as F, types as T

import elmo_geo
from elmo_geo.io import to_gpq, load_sdf, download_link

elmo_geo.register()

# COMMAND ----------

os_schema = T.StructType([
    T.StructField('layer', T.StringType(), True),
    T.StructField('theme', T.StringType(), True),
    T.StructField('description', T.StringType(), True),
    T.StructField('watermark', T.StringType(), True),
    T.StructField('width', T.DoubleType(), True),
    T.StructField('geometry', T.BinaryType(), True),
])

sdf_os = (spark.read.format('parquet')
    .schema(os_schema)
    .load('dbfs:/mnt/lab/restricted/ELM-Project/stg/os-ngd-2022.parquet')
    .select(
        'layer',
        'theme',
        'description',
        'width',
        F.expr('ST_SetSRID(ST_GeomFromWKB(geometry), 27700) AS geometry'),
    )
    .withColumn('geometry', F.expr('CASE WHEN (width IS NOT NULL) THEN ST_Buffer(geometry, width) ELSE geometry END'))
    .drop('width')
    # .withColumn('gtype', F.expr('ST_GeometryType(geometry)'))
)

sdf_os_wall = (sdf_os
    .filter('description REGEXP " Wall"')
    .transform(to_gpq, 'dbfs:/mnt/lab/restricted/ELM-Project/ods/os-wall-2022.parquet')
)


# COMMAND ----------

sdf_os_water = spark.read.format('geoparquet').load('dbfs:/mnt/lab/restricted/ELM-Project/ods/os-water-2022.parquet')

# COMMAND ----------

f = '/tmp/os-water-2022.gpkg'
gdf = gpd.GeoDataFrame(sdf_os_water.toPandas(), crs=27700)

gdf.to_file(f)
download_link(f)

# COMMAND ----------

limit = 20

df = (sdf_os_water
    .withColumn('gtype', F.expr('SUBSTRING(ST_GeometryType(ST_Multi(geometry)), 8)'))
    .groupby('description', 'gtype').count()
    .toPandas()
    .unstack('gtype').fillna(0)
    .assign(total = lambda df: df.sum(axis=1)).sort_values('total', ascending=False).drop(columns=['total'])  # Sort by Total
    .iloc[:limit]
)
df.columns = df.columns.droplevel()

df.plot(stacked=True, kind='barh')

# COMMAND ----------

print(*sorted(df['description'].unique().tolist()), sep=',')

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs/mnt/base/restricted/source_ordnance_survey_data_hub
# MAGIC find . -type f | parallel 'du -sh {}'

# COMMAND ----------

import geopandas as gpd

f = '/dbfs/mnt/base/restricted/source_ordnance_survey_data_hub/dataset_ngd_land_features/format_GPKG_ngd_land_features/LATEST_ngd_land_features/lnd_fts_landpoint.gpkg'
gdf = gpd.read_file(f)
gdf.plot()
