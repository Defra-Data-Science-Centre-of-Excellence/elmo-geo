# Databricks notebook source
# MAGIC %md
# MAGIC # Land Use Change:  Woodland versus Hedgerow
# MAGIC Assuming 100% uptake on parcels EVAST have identified for woodland creation, what is the loss of hedgerow.
# MAGIC
# MAGIC > m_hedge / hectare_parcel  (mean & median)
# MAGIC > m_hedge_lost = ha_ewco / hectare_parcel * m_hedge
# MAGIC > 
# MAGIC > 228_000 ha woodland creation
# MAGIC > 250_000 peatland (80% lowland)
# MAGIC > 
# MAGIC > | Scenario | Hedgerow Loss |
# MAGIC > | --- | --- |
# MAGIC > | Even | . |
# MAGIC > | 75% LFA | . |
# MAGIC > | 50% LFA | . |
# MAGIC > | 25% LFA | . |
# MAGIC > | 75% Livestock | . |
# MAGIC > | 50% Even | . |
# MAGIC > | 75% Arable | . |
# MAGIC
# MAGIC
# MAGIC ### Data
# MAGIC - EVAST, Woodland Uptake _(a list of parcels predicted to uptake ELM woodland creation options)_
# MAGIC - WFM, Farm _(for farm type)_
# MAGIC - WFM, Parcel _(for total parcels and UAA)_
# MAGIC - ELMSE, Hedgerows _(for parcel length of hedgerows)_
# MAGIC - ELMO, LFA _(for parcel lfa status)_
# MAGIC
# MAGIC ### Summary
# MAGIC
# MAGIC |          |     Target     | 100% Uptake |
# MAGIC | -------: | -------------: | ----------: |
# MAGIC | Woodland |   + 228,000 ha |     + 43.2% |
# MAGIC | Hedgerow | + 75,000,000 m |      - 9.8% |
# MAGIC
# MAGIC |        Metric        |      Group     |    Sum    | Proportion | Median |
# MAGIC | :------------------- | :------------- | --------: | ---------: | -----: |
# MAGIC | Woodland Uptake (ha) | Total          |    98,467 |       1.0% |
# MAGIC |                      | Livestock Farm |    36,771 |      37.3% |
# MAGIC |                      | LFA Parcel     |    12,011 |      12.2% |
# MAGIC | Hedgerow Loss (m)    | Total          | 7,322,407 |       1.7% |
# MAGIC |                      | Livestock Farm | 2,896,988 |      39.6% |
# MAGIC |                      | LFA Parcel     |   669,801 |       9.1% |
# MAGIC

# COMMAND ----------

import pandas as pd
pd.options.display.float_format = '{:,.3f}'.format


f_wfm = '/dbfs/mnt/lab/unrestricted/elm/wfm/2023_06_09/wfm_parcels.feather'
f_hedge = '/dbfs/mnt/lab/unrestricted/elm/buffer_strips/hedgerows.feather'
f_evast = '/dbfs/mnt/lab/unrestricted/elm_data/evast/woodland_uptake/2023_07_12.csv'
f_lfa = '/dbfs/mnt/lab/unrestricted/elm/elmo/lfa/lfa.feather'
f_farm = '/dbfs/mnt/lab/unrestricted/elm/wfm/2023_06_09/wfm_farms.feather'


df_wfm = pd.read_feather(f_wfm)
df_hedge = pd.read_feather(f_hedge)
df_evast = pd.read_csv(f_evast)
df_lfa = pd.read_feather(f_lfa)
df_farm = pd.read_feather(f_farm)


# COMMAND ----------

wfm = (df_wfm
  [['id_parcel', 'ha_parcel_uaa']]
)
hedge = (df_hedge
  [['id_parcel', 'm_adj']]
  .fillna(0)
)
evast = pd.DataFrame({
  'id_parcel': df_evast['x'],
  'woodland_uptake': True,
})
lfa = (df_lfa
  .assign(lfa = lambda df: 0<df['proportion'])
  [['id_parcel', 'lfa']]
)
farm_type = (df_farm
  [['id_business', 'farm_type']]
  .assign(
    farm_type = lambda df: df['farm_type'].map({
        0: None,
        1: 'Arable',  # Cereals
        2: 'Arable',  # General Cropping
        6: 'Livestock',  # Dairy
        7: 'Livestock',  # LFA Grazing
        8: 'Livestock',  # Lowland Grazing
        9: 'Mixed',
      }).fillna('None')
  )
  .merge(df_wfm[['id_business', 'id_parcel']])
  [['id_parcel', 'farm_type']]
)

df = (wfm
  .merge(hedge, how='left')
  .merge(evast, how='left')
  .merge(lfa, how='left')
  .merge(farm_type, how='left')
  .assign(
    woodland_uptake = lambda df: df['woodland_uptake'].fillna(False),
    m_per_ha_hedge = lambda df: df['m_adj'] / df['ha_parcel_uaa'],
    lfa = lambda df: df['lfa'].fillna(False),
  )
)


df

# COMMAND ----------

p = (df
  .pivot_table(
    index = ['id_parcel'],
    columns = ['woodland_uptake', 'lfa', 'farm_type'],
    values = ['ha_parcel_uaa', 'm_adj'],
  )
  .agg(['sum', 'median', 'mean'])
  .reset_index()
  # .pivot_table(
  #   index = ['woodland_uptake', 'lfa', 'farm_type'],
  #   columns = 'level_0',
  #   values = 0,
  # )
  # .reset_index()
)

p

# COMMAND ----------

a, b = p['ha_parcel_uaa'].sum(), p['m_adj'].sum()

x = p.query('woodland_uptake == True')
c, d = x['ha_parcel_uaa'].sum(), x['m_adj'].sum()

y = x.query('lfa == True')
e, f = y['ha_parcel_uaa'].sum(), y['m_adj'].sum()

y = x.query('farm_type == "Livestock"')
g, h = y['ha_parcel_uaa'].sum(), y['m_adj'].sum()

y = x.query('farm_type == "Arable"')
i, j = y['ha_parcel_uaa'].sum(), y['m_adj'].sum()

y = x.query('farm_type == "Mixed"')
k, l = y['ha_parcel_uaa'].sum(), y['m_adj'].sum()


pd.DataFrame({
  'Metric': ['Woodland Uptake (ha)', 'Hedgerow Loss (m)', 'Woodland Uptake (ha)', 'Hedgerow Loss (m)', 'Woodland Uptake (ha)', 'Hedgerow Loss (m)', 'Woodland Uptake (ha)', 'Hedgerow Loss (m)', 'Woodland Uptake (ha)', 'Hedgerow Loss (m)'],
  'Group': ['Total', 'Total', ' LFA Parcel', ' LFA Parcel', 'Livestock Farm', 'Arable Farm', 'Arable Farm', 'Mixed Farm', 'Mixed Farm', 'Livestock Farm'],
  'Sum': pd.Series([c, d, e, f, g, h, i, j, k, l]).map('{:,.0f}'.format),
  'Proportion': pd.Series([c/a, d/b, e/c, f/d, g/c, h/d, i/c, j/d, k/c, l/d]).map('{:,.1%}'.format),
}).set_index(['Metric', 'Group']).sort_index()
