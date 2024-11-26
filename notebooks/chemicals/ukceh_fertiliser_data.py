# Databricks notebook source
# MAGIC %md
# MAGIC # UKCEH Fertiliser Data
# MAGIC
# MAGIC Looks at the output of the UKCEH Crop Map Plus Fertilisers data.
# MAGIC The raw data was joined into RPA parcels for Nitrogen, Phospherous and Potassium (NPK) and converted to kg/ha.
# MAGIC The figures are estimates of annual application rates for 2010-2015 and are based on the British Survey of Fertiliser Practice (PSFP).
# MAGIC Where there were missing values in the 1km2 gridded product, we have imputed the nearest value.
# MAGIC
# MAGIC - By: edward.burrows@defra.gov.uk
# MAGIC - Date: 25th November 2024

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from elmo_geo import register
from elmo_geo.datasets import ukceh_fertilisers_parcels

register()

df = ukceh_fertilisers_parcels.pdf()

sns.set_theme(context="notebook", style="white", palette="Dark2")
fig, ax = plt.subplots(figsize=(10, 6))

for i, c in enumerate(["nitrogen_kg_ha", "phosphorus_kg_ha", "potassium_kg_ha"]):
    sns.histplot(df[c], stat="probability", bins=10, binrange=(0, 200), label=c.split("_")[0].title(), element="step", alpha=0.4)
    ax.margins(x=0)
    ax.set_frame_on(False)
    ax.legend()
    ax.set_title("UKCEH fertiliser application by parcel, 2010-2015, kg/ha per annum.", loc="left")
    ax.set_xlabel("Application rate kg/ha per annum")


# COMMAND ----------

# correlation plot
fig, ax = plt.subplots(figsize=(8, 8))
corr = df.set_index("id_parcel").corr().rename(lambda c: c.split("_")[0].title(), axis=0).rename(lambda c: c.split("_")[0].title(), axis=1)
matrix = np.triu(corr)
sns.heatmap(corr, annot=True, vmin=0, vmax=1, cmap="Blues", mask=matrix, ax=ax)
ax.set_title("Pearson correlation coefficients between application rates.")
fig.show()

# COMMAND ----------
