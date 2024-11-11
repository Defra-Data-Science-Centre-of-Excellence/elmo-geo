# Databricks notebook source
import pyspark.sql.functions as F
from pathlib import Path
from elmo_geo.io.download import download_link
import itertools
import os
import pandas as pd

from elmo_geo import register
from elmo_geo.datasets import catalogue
from elmo_geo.datasets import (
    esc_species_parcels,
    esc_carbon_parcels,
)

from elmo_geo.etl.transformations import combine_long, sjoin_parcels, sjoin_parcel_proportion
from elmo_geo.st.join import sjoin

register()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Exporting ESC-Carbine data for Forest Research
# MAGIC
# MAGIC Datasets need to be filtered to single woodlant type + rcp scenario so that file sizes are manageable.
# MAGIC
# MAGIC For the species data, the dataset is pivoted from long to wide format so that there is a column for each metric (yield class, suitability, and area) for each species.
# MAGIC

# COMMAND ----------

woodland_types = [
    "productive_conifer",
    "native_broadleaved",
    "riparian",
    "silvoarable",
    "wood_pasture",
]
rcps = [26, 45, 60, 85]
paths = []
for wt, rcp in itertools.product(woodland_types, rcps):
    f = Path(esc_carbon_parcels.path)
    f_out = "/dbfs/FileStore/elmo-geo-downloads/esc-forest-research-export/" + f"{wt}-{rcp}-{f.name}"
    f_out_csv = f_out.replace(".parquet", ".csv")

    if os.path.exists(f_out):
        os.remove(f_out)
    if os.path.exists(f_out_csv):
        os.remove(f_out_csv)

    df = esc_carbon_parcels.sdf().filter(f"(woodland_type = '{wt}') and (rcp = {rcp})").toPandas()
    df.to_parquet(f_out)
    df.to_csv(f_out_csv, index=False)
    paths.append(f_out)
    paths.append(f_out_csv)

[displayHTML(download_link(f_out)) for f_out in paths]

# COMMAND ----------

from elmo_geo.etl.transformations import pivot_wide_sdf

groupby_cols = [
    "id_parcel",
    "nopeat_area",
    "woodland_type",
    "rcp",
    "period_AA_T1",
    "period_T2",
    "period_AA_T1_duration",
    "period_T2_duration",
]

sdf_species = esc_species_parcels.sdf().drop("tiles")

species = sdf_species.select("species").dropDuplicates().toPandas()["species"]

sdf_wide = (pivot_wide_sdf(sdf_species.drop("yield_class", "suitability"), name_col = "species", value_col = "area")
 .withColumnsRenamed(dict(zip(species, [f"{s}_area" for s in species])))
 .dropDuplicates()
 .join(pivot_wide_sdf(sdf_species.drop("yield_class", "area"), name_col = "species", value_col = "suitability")
       .withColumnsRenamed(dict(zip(species, [f"{s}_suitability" for s in species])))
       .dropDuplicates(), on = groupby_cols)
 .join(pivot_wide_sdf(sdf_species.drop("area", "suitability"), name_col="species", value_col="yield_class")
       .withColumnsRenamed(dict(zip(species, [f"{s}_yield_class" for s in species])))
       .dropDuplicates(), on = groupby_cols)
)

# COMMAND ----------

sdf_wide.display()

# COMMAND ----------

sdf_wide.filter("(id_parcel = 'NT68004813') and (woodland_type = 'silvoarable') and (rcp = 85) and (period_T2 = '2021_2028')").display()

# COMMAND ----------

assert sdf_wide.count() == sdf_wide.select("id_parcel", "woodland_type", "rcp", "period_AA_T1", "period_T2").dropDuplicates().count()

# COMMAND ----------

woodland_types = [
    "productive_conifer",
    "native_broadleaved",
    "riparian",
    "silvoarable",
    "wood_pasture",
]
rcps = [26, 45, 60, 85]
paths = []
for wt, rcp in itertools.product(woodland_types, rcps):
    f = Path(esc_species_parcels.path)
    f_out = "/dbfs/FileStore/elmo-geo-downloads/esc-forest-research-export/" + f"{wt}-{rcp}-{f.name}"
    f_out_csv = f_out.replace(".parquet", ".csv")

    if os.path.exists(f_out):
        os.remove(f_out)
    if os.path.exists(f_out_csv):
        os.remove(f_out_csv)

    df = sdf_wide.filter(f"(woodland_type = '{wt}') and (rcp = {rcp})").toPandas()
    df.to_parquet(f_out)
    df.to_csv(f_out_csv, index=False)

    paths.append(f_out)
    paths.append(f_out_csv)

[displayHTML(download_link(f_out)) for f_out in paths]
