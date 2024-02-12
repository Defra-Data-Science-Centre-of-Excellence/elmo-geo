# Databricks notebook source
import numpy as np
from scipy.stats import linregress
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

import elmo_geo
from elmo_geo.io import load_sdf

elmo_geo.register()

# COMMAND ----------

sdf = load_sdf('rpa-parcels-adas')

df = sdf.select(
    F.expr('Shape_Area/10000 AS ha'),
    F.expr('Shape_Length AS m'),
).toPandas()
df_central = df[abs(df['ha']-df['ha'].mean()) < df['ha'].std()].rename(columns={'ha':'area (ha)','m':'perimeter (m)'}).iloc[:1_000]


model = linregress(df['m'], df['ha'])


g = sns.jointplot(
    data = df_central,
    x = 'area (ha)',
    y = 'perimeter (m)',
    kind = 'reg',
    scatter_kws = {'s': 1},
    line_kws = {'color': 'k'},
)
plt.suptitle(f'Parcel Area vs Perimeter\nOLR:  $perimeter (m) = {model.slope:.1f} \\times area (ha) + {model.intercept:.1f}$\nP: {model.pvalue:.3f},  R²: {model.rvalue:.3f}')
None

# COMMAND ----------


