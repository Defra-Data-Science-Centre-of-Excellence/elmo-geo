# Databricks notebook source
# MAGIC %md
# MAGIC # Arable Incentivisation
# MAGIC How many arable farmers are incentivised to put 100% of their land in SFI?
# MAGIC

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F


ha_arable = ['ha_fallow','ha_field_beans','ha_fodder_maize','ha_grain_maize','ha_other_crop','ha_peas','ha_potatoes','ha_oilseed_rape','ha_spring_barley','ha_spring_oats','ha_spring_wheat','ha_winter_barley','ha_winter_oats','ha_winter_wheat']
ha_grassland = ['ha_disadvantaged','ha_fenland','ha_lowland_other','ha_improved_disadvantaged','ha_improved_grades_1_2','ha_improved_grades_3_4_5','ha_moorland','ha_severely_disadvantaged','ha_temporary_pasture','ha_unimproved','ha_unimproved_disadvantaged']
gm_arable = [f'gm_{col[3:]}' for col in ha_arable if col not in ['ha_fallow',]]
gm_grassland = [f'gm_{col[3:]}' for col in ha_grassland if col not in []]


sdf_wfm = spark.read.parquet('dbfs:/mnt/lab/restricted/ELM-Project/stg/wfm-field-2024_01_26.parquet')


display(sdf_wfm)

# COMMAND ----------

sdf_parcel = sdf_wfm.selectExpr(
    'id_business', 'id_parcel',
    f"{'+'.join(ha_arable)} AS ha_arable",
    f"{'+'.join(ha_grassland)} AS ha_grassland",
    f"{'+'.join(gm_arable)} AS gm_arable",
    f"{'+'.join(gm_grassland)} AS gm_grassland",
    f"ha_arable + ha_grassland AS ha_total",
)

sdf_business = (sdf_parcel
    .groupby('id_business').agg(
        F.expr('SUM(ha_arable) / SUM(ha_arable + ha_grassland) AS p_arable',
    )
)

sdf = (sdf_parcel
    .join(sdf_business, on='id_business')
    .selectExpr(
        'id_business', 'id_parcel',
        '(gm_arable + gm_grassland) / (ha_arable + ha_grassland) AS gmph',
        'gm_arable / ha_arable AS gmph_arable',
        'p_arable',
    )
)


sdf.display()

# COMMAND ----------

df = pd.concat(sdf
    .filter(f'{arable_ness} <= p_arable')
    .withColumn('gmph_arable', F.expr('CASE WHEN 0==gmph_arable THEN null ELSE gmph_arable END' if null_fallow else 'gmph_arable'))
    .groupby('id_business').agg(
        F.expr('COUNT(id_parcel) AS n_parcel'),
        F.expr('SUM(CASE WHEN gmph_arable IS null THEN 0 ELSE 1 END) AS n_parcel_not_null'),
        F.expr(f'CAST(NVL(max(gmph_arable), 0) < {payment_rate} AS INT) AS max_all'),
        F.expr(f'CAST({payment_rate} < NVL(min(gmph_arable), 0) AS INT) AS min_none'),
    )
    .groupby().agg(
        F.lit('null' if null_fallow else '0').alias('0 gmph as'),
        F.lit(payment_rate).alias('payment rate'),
        F.expr(f'"{arable_ness:.1f}" AS business_arableness'),
        F.expr('COUNT(id_business) AS n_business'),
        F.expr('SUM(n_parcel) AS n_parcel'),
        F.expr('SUM(n_parcel_not_null) AS n_parcel_not_null'),
        F.expr('MEAN(max_all) AS p_all'), F.expr('SUM(max_all) AS n_all'),
        F.expr('MEAN(min_none) AS p_none'), F.expr('SUM(min_none) AS n_none'),
    )
    .toPandas()
    for null_fallow in [True, False]
    for payment_rate in [577, 662, 747, 831, 916]
    for arable_ness in [0.0, 0.5, 0.9, 1.0]
)


display(df)

# COMMAND ----------

sdf_wfm.selectExpr(
    'id_business', 'id_parcel',
    f"{'+'.join(ha_arable)} AS ha_arable",
    f"{'+'.join(ha_grassland)} AS ha_grassland",
    f"{'+'.join(gm_arable)} AS gm_arable",
    f"{'+'.join(gm_grassland)} AS gm_grassland",
    # f"ha_arable + ha_grassland AS ha_total",
).groupby('id_business').agg(
    F.expr('SUM(gm_arable) / SUM(ha_arable) AS gmph_arable'),
    F.expr('SUM(gm_grassland) / SUM(ha_grassland) AS gmph_grassland'),
).groupby().agg(
    F.expr('MEAN(gmph_arable)'),
    F.expr('MEAN(gmph_grassland)'),
    F.expr('MEDIAN(gmph_arable)'),
    F.expr('MEDIAN(gmph_grassland)'),
).display()
