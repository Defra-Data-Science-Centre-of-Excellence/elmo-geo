# Databricks notebook source
# MAGIC %md
# MAGIC # Riparian Corridors
# MAGIC
# MAGIC ### Planning
# MAGIC - join overland_flow with boundary segments
# MAGIC - merge Crispins scores per segment
# MAGIC - multiply by MaxAcc
# MAGIC - stretch goal: use land_cover for interior segments
# MAGIC - stretch goal: network effects - connectivity score
# MAGIC

# COMMAND ----------


import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

from elmo_geo import register
from elmo_geo.io import to_gdf
from elmo_geo.plot.base_map import plot_gdf
from elmo_geo.st import sjoin
from elmo_geo.utils.misc import load_sdf

register()



f_segment = "/dbfs/mnt/lab/restricted/ELM-Project/silver/elmo_geo-boundary_segment-2024_06_21.parquet"
f_olf = "/dbfs/mnt/lab/restricted/ELM-Project/bronze/ea-overland_flow-2024_06_19_direct.parquet"



# COMMAND ----------

gdf = pd.read_parquet(f_olf).pipe(to_gdf)


plot_gdf(gdf)
gdf

# COMMAND ----------

sdf = (
    load_sdf(f_segment)
    .select(
        "id_parcel",
        F.monotonically_increasing_id().alias("id_segment"),
        "geometry",
    )
    .transform(sjoin, load_sdf(f_olf))
    .groupby("id_parcel", "id_segment")
    .agg(
        F.collect_set("PermID").alias("PermIDs"),
        F.collect_set("OPERATIONAL_CATCHMENT").alias("catchments"),
        F.collect_set("WATERBODY_NAME").alias("subcatchments"),
        # *Risk columns are formatted "[1-5]. [description]".
        F.expr(
            """SUM(
            MaxFlowAcc * (
                CAST(SUBSTRING(CatchmentRiskDesc, 1, 2) AS DOUBLE)
                + CAST(SUBSTRING(LandUseRisk, 1, 2) AS DOUBLE)
                + CAST(SUBSTRING(SlopeRisk, 1, 2) AS DOUBLE)
                + CAST(SUBSTRING(CombinedSoilRisk, 1, 2) AS DOUBLE)
                + CAST(SUBSTRING(ReceptorDistanceRisk, 1, 2) AS DOUBLE)
            ) / (5 * 5 * 10000)
        )"""
        ).alias("score"),  # 5*5*10,000 = normalising 5 values with max score 5 from sqm to ha
    )
)


df = sdf.toPandas()
sdf.display()

# COMMAND ----------

df.hist("score", log=True, edgecolor="k")
plt.gca().grid(False)
