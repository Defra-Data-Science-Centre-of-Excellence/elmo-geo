# Databricks notebook source
# MAGIC %md
# MAGIC # Total Distributions
# MAGIC
# MAGIC This notebook visualises the total distribution of a given years bare soil
# MAGIC with respect to whether a parcel is arable or grassland.
# MAGIC [explain more here]

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %pip install -U beautifulsoup4 lxml

# COMMAND ----------

from elmo_geo.plot_bare_soil_dist import plot_bare_soil_dist
from elmo_geo.sentinel import sentinel_years

dbutils.widgets.dropdown("year", sentinel_years[-1], sentinel_years)

# COMMAND ----------

year = int(dbutils.widgets.get("year"))
path = f"/mnt/lab/unrestricted/elm/elmo/baresoil/output-{year}.parquet"

# COMMAND ----------

df = spark.read.parquet(path).toPandas()
fig, ax = plot_bare_soil_dist(
    data=df.bare_soil_percent,
    title=(
        "Distribution of parcels in England by bare soil cover "
        f"November {year-1} - February {year}"
    ),
)
fig.show()

# COMMAND ----------

# # spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
# spark.conf.get("spark")


# COMMAND ----------

# Import WFM data for info on fields
wfm_path = "/mnt/lab/unrestricted/edward.burrows@defra.gov.uk/parcels_v2.parquet"
cols = spark.read.parquet(wfm_path).columns
cols = [col for col in cols if col in ("id_business", "id_parcel") or col.startswith("ha_")]
wfm = spark.read.parquet(wfm_path).select(*cols).repartition(100).toPandas()
wfm = wfm.set_index("id_parcel").join(df.set_index("id_parcel"), how="left")
wfm

# COMMAND ----------


# function to grab a subsection of wfm  parcels (used in baseline_analysis)
def add_bool_col(df, col_list, new_col_name):
    """Adds a boolean column to dataframe."""
    df[new_col_name] = df[filter(col_list, df.columns)].sum(axis=1) > 0
    return df


# COMMAND ----------

# arable soils analysis
predicate = lambda col: col.startswith("ha_arable_")
wfm["arable"] = wfm[filter(predicate, wfm.columns)].sum(axis=1) > 0
fig, ax = plot_bare_soil_dist(
    data=wfm.loc[wfm.arable, "bare_soil_percent"],
    title=(
        "Distribution of arable parcels in England by bare soil cover "
        f"November {year-1} - February {year}"
    ),
)
fig.show()

# COMMAND ----------

# improved grassland soils analysis
predicate = lambda col: col in [
    "ha_grassland_temporary_improved_grades_1_2",
    "ha_grassland_temporary_improved_grades_3_plus",
    "ha_grassland_improved_disadvantaged",
    # "ha_arable_grassland_temporary",
]
wfm["improved_grassland"] = wfm[filter(predicate, wfm.columns)].sum(axis=1) > 0
fig, ax = plot_bare_soil_dist(
    data=wfm.loc[wfm.improved_grassland, "bare_soil_percent"],
    title=(
        "Distribution of improved grassland parcels in England by bare soil "
        f"cover November {year-1} - February {year}"
    ),
)
fig.show()

# COMMAND ----------
