# Databricks notebook source
import numpy as np
import pandas as pd
from pyspark.sql import functions as F, types as T
import elmo_geo
from elmo_geo.io import load_sdf
from elmo_geo.st.udf import st_union

elmo_geo.register()

# COMMAND ----------

sdf_wfm = pd.read_feather('/dbfs/mnt/lab/restricted/ELM-Project/stg/wfm-field-2023_11_28.feather', columns=['id_business', 'id_parcel']).pipe(spark.createDataFrame)
sdf_parcel = load_sdf('rpa-parcel-2023_12_13').select('id_parcel', 'geometry')
# sdf_hedge = load_sdf('rpa-hedge-2023_12_13').select('id_parcel', 'geometry')
sdf_hedge = load_sdf('elmo_geo-hedge').select('id_parcel', 'geometry')

# COMMAND ----------

sdf_business = (sdf_wfm
    .join(sdf_parcel, on='id_parcel', how='full')
    .transform(st_union, 'id_business')
)

# sdf_business.write.format('noop').save(mode='overwrite')

# COMMAND ----------

sdf = (sdf_wfm
    # .join(sdf_business.withColumnRenamed('geometry', 'geometry_business'), on='id_business', how='full')
    .join(sdf_parcel.withColumnRenamed('geometry', 'geometry_parcel'), on='id_parcel', how='full')
    .join(sdf_hedge.withColumnRenamed('geometry', 'geometry_hedge'), on='id_parcel', how='full')
    .select(
        'id_business',
        'id_parcel',
        # F.expr('NVL(geometry_business, ST_GeomFromText("Point EMPTY")) AS geometry_business'),
        F.expr('NVL(geometry_parcel, ST_GeomFromText("Point EMPTY")) AS geometry_parcel'),
        F.expr('NVL(geometry_hedge, ST_GeomFromText("Point EMPTY")) AS geometry_hedge'),
    )
    .withColumn('geometry_hedge', F.expr('EXPLODE(ST_Dump(geometry_hedge))'))
)


display(sdf)

# COMMAND ----------

df = sdf.select(
    'id_business',
    'id_parcel',
    # F.expr('ST_Distance(geometry_business, geometry_hedge) AS m_from_business'),
    F.expr('ST_Distance(geometry_parcel, geometry_hedge) AS m_from_parcel'),
).toPandas()


df

# COMMAND ----------

df[500 < df['m_from_parcel']]

# COMMAND ----------

df.set_index('id_business').plot(kind='hist', logy=True, bins=100, legend=False, xlabel='Distance (meters)', title='Distance between Hedges and their assigned Parcel\nData Extracts 2023-12-13, Log Y-Axis')

# COMMAND ----------

sdf = spark.read.parquet('dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-hedge-2023_12_13.parquet')
sdf.selectExpr(
    'CAST(LENGTH AS float) AS m',
    'CAST(ADJACENT_ELIGIBLE_FOR_EFA IS NOT NULL AS int) AS adj',
    'm * (adj+1)/2 AS m_adj',
).groupby().sum().display()

# COMMAND ----------

from compress_brotli import compress

df = spark.range(0, 1000).toDF("number") 


df.write.parquet("data/snappy", compression='snappy')
df.write.parquet("data/lz4", compression='lz4')
df.write.parquet("data/zstd", compression='zstd')
df.write.parquet("data/zlib", compression='zlib')
df.write.parquet("data/brotli", compression=compress)

# COMMAND ----------

# MAGIC %md # Stuff

# COMMAND ----------

ha_arable = ['ha_business_pasture_and_crops', 'ha_business_permanent_crops', 'ha_business_crops']
ha_grass = ['ha_business_permament_pasture', 'ha_business_fodder']

df_wfm_farm = pd.read_feather('/dbfs/mnt/lab/restricted/ELM-Project/stg/wfm-farm-2023_11_28.feather', columns=['id_business', *ha_arable, *ha_grass])
df_wfm_field = pd.read_feather('/dbfs/mnt/lab/restricted/ELM-Project/stg/wfm-field-2023_11_28.feather', columns=['id_business', 'id_parcel'])
df_parcel = load_sdf('rpa-parcel-adas').selectExpr('id_parcel', 'ST_Area(geometry)/10000 AS ha_parcel_geo').toPandas()
df_sssi = pd.read_parquet('/dbfs/mnt/lab/unrestricted/elm/elmo/sssi/output.parquet', columns=['id_parcel', 'proportion']).rename(columns={'proportion': 'p_sssi'})
df_lfa = pd.read_parquet('/dbfs/mnt/lab/unrestricted/elm/elmo/lfa/output.parquet', columns=['id_parcel', 'proportion']).rename(columns={'proportion': 'p_lfa'})

# COMMAND ----------

f"{df_parcel['ha_parcel_geo'].sum():,.0f}"

# COMMAND ----------

df = (df_wfm_field
    .merge(df_parcel, on='id_parcel', how='outer')
    .merge(df_sssi, on='id_parcel', how='outer')
    .merge(df_lfa, on='id_parcel', how='outer')
    .assign(
        count = 1,
        ha_lfa = lambda df: df['ha_parcel_geo'] * df['p_lfa'],
        ha_sssi = lambda df: df['ha_parcel_geo'] * df['p_sssi'],
    )
    .groupby('id_business')[['ha_parcel_geo', 'ha_lfa', 'ha_sssi', 'count']].sum()
    .merge(df_wfm_farm, on='id_business', how='outer')
    .assign(
        ha_arable = lambda df: df[ha_arable].sum(axis=1),
        ha_grass = lambda df: df[ha_grass].sum(axis=1),
    )
    .assign(
        is_upland = lambda df: df['ha_parcel_geo']/2 < df['ha_lfa'],
        is_sssi = lambda df: 0 < df['ha_sssi'],
        is_arable = lambda df: df['ha_grass'] < df['ha_arable'],
        is_grass = lambda df: df['ha_arable'] < df['ha_grass'],
        ha = lambda df: df['ha_parcel_geo'],
    )
    .drop(columns=[*ha_arable, *ha_grass, 'ha_parcel_geo'])
    # [['id_business', 'ha', 'is_upland', 'is_sssi', 'is_arable', 'is_grass']]
    .sort_values('ha')
)

pd.options.display.float_format = '{:,.1f}'.format
df

# COMMAND ----------



# COMMAND ----------

pd.Series(np.log10(df['ha'])//1).value_counts().sort_index()

# COMMAND ----------

display(pd.DataFrame({
    'upland median': df_business.query('is_upland')[['ha']].median(),
    'upland mean': df_business.query('is_upland')[['ha']].mean(),
    'upland mean*': df_business.query('ha<1e4').query('is_upland')[['ha']].mean(),
    'sssi median': df_business.query('is_sssi')[['ha']].median(),
    'sssi mean': df_business.query('is_sssi')[['ha']].mean(),
    'sssi mean*': df_business.query('ha<1e4').query('is_sssi')[['ha']].mean(),
    'arable median': df_business.query('is_arable')[['ha']].median(),
    'arable mean': df_business.query('is_arable')[['ha']].mean(),
    'arable mean*': df_business.query('ha<1e4').query('is_arable')[['ha']].mean(),
    'grass median': df_business.query('is_grass')[['ha']].median(),
    'grass mean': df_business.query('is_grass')[['ha']].mean(),
    'grass mean*': df_business.query('ha<1e4').query('is_grass')[['ha']].mean(),
    'total median': df_business[['ha']].median(),
    'total mean': df_business[['ha']].mean(),
    'total mean*': df_business.query('ha<1e4')[['ha']].mean(),
}).T.round(5).reset_index())

# COMMAND ----------

df_business.query('ha>1e4')

# COMMAND ----------

df = gpd.read_parquet('/dbfs/mnt/lab/restricted/ELM-Project/stg/os-field_boundary-sample_v1_2.parquet')
df

# COMMAND ----------

df.explore()
