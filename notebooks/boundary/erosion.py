# Databricks notebook source
# MAGIC %md
# MAGIC # Riparian Corridors
# MAGIC
# MAGIC ### Planning
# MAGIC - join overland_flow with boundary segments
# MAGIC - create a score
# MAGIC - review the score
# MAGIC - stretch goal: use land_cover for interior segments
# MAGIC - stretch goal: network effects - connectivity score
# MAGIC
# MAGIC
# MAGIC ### Output
# MAGIC - id_parcel, id_segment|None, *RiskScore, MaxFlowAcc, catchment, subcatchement, score
# MAGIC - Plot of Wye Valley, floods likely, pollution high
# MAGIC - Plot of North Pennines, floods unlikely, pollution low
# MAGIC

# COMMAND ----------


import pandas as pd
from pyspark.sql import functions as F

from elmo_geo import register
from elmo_geo.io import download_link, to_gdf
from elmo_geo.plot.base_map import plot_gdf
from elmo_geo.utils.misc import load_sdf

register()


f_parcel = "/dbfs/mnt/lab/restricted/ELM-Project/silver/rpa-parcel-adas.parquet"
f_segment = "/dbfs/mnt/lab/restricted/ELM-Project/silver/elmo_geo-boundary_segment-2024_06_21.parquet"
f_olf = "/dbfs/mnt/lab/restricted/ELM-Project/bronze/ea-overland_flow-2024_06_19_direct.parquet"

f = "/dbfs/mnt/lab/restricted/ELM-Project/gold/elmo_geo-parcel_olf-2024_07_11_beta.parquet"

# COMMAND ----------

# Explore
gdf = pd.read_parquet(f_olf).pipe(to_gdf)


plot_gdf(gdf)
gdf

# COMMAND ----------

sdf_parcel = load_sdf(f_parcel).select(
    "id_parcel",
    "geometry",
    "sindex",
)

sdf_segment = load_sdf(f_segment).select(
    "id_parcel",
    F.monotonically_increasing_id().alias("id_segment"),
    F.expr("ST_Area(geometry)").alias("m"),
    "geometry",
)

sdf_olf = load_sdf(f_olf).selectExpr(
    "PermID AS fid",
    "OPERATIONAL_CATCHMENT AS catchment",
    "WATERBODY_NAME AS subcatchment",
    "CAST(SUBSTRING(CatchmentRiskDesc, 1, 2) AS INT) AS CatchmentScore",
    "CAST(SUBSTRING(LandUseRisk, 1, 2) AS INT) AS LandUseScore",
    "CAST(SUBSTRING(SlopeRisk, 1, 2) AS INT) AS SlopeScore",
    "CAST(SUBSTRING(SoilErosion, 1, 2) AS INT) AS SoilErosionScore",
    "CAST(SUBSTRING(SoilRunoff, 1, 2) AS INT) AS SoilRunoffScore",
    "CAST(SUBSTRING(CombinedSoilRisk, 1, 2) AS INT) AS CombinedSoilScore",
    "CAST(SUBSTRING(ReceptorDistanceRisk, 1, 2) AS INT) AS ReceptorDistanceScore",
    "CAST(SUBSTRING(MeanRainfalRisk, 1, 2) AS INT) AS MeanRainfallScore",
    # "MajLandUse", "MeanSlope", "Slope1haWatershed", "MinFlowWater", "MinFlowRoad", "SSSI_Intersect", "FlowAccClass", "MaxFlowAcc",
    "NTILE(5) OVER (ORDER BY MaxFlowAcc) AS AreaScore",  # Replace *MaxFlowAcc with +Quintile(MaxFlowAcc)
    "(CatchmentScore + LandUseScore + SlopeScore + CombinedSoilScore + MeanRainfallScore + AreaScore) /5/6 AS score",
    "Shape_Length AS length",
    "geometry",
)


sdf_parcel.createOrReplaceTempView("parcel")
sdf_olf.createOrReplaceTempView("olf")
sdf_segment.createOrReplaceTempView("segment")

# COMMAND ----------

sdf = spark.sql(
    """
    SELECT parcel.*, olf.* EXCEPT(olf.geometry), olf.geometry AS geometry_olf
    FROM parcel JOIN olf
    ON ST_Intersects(parcel.geometry, olf.geometry)
"""
)


sdf.display()
sdf.count()

# COMMAND ----------

pdf = sdf.drop("geometry", "geometry_olf").toPandas()
pdf.to_parquet(f)
download_link(f)

# COMMAND ----------

gdf = to_gdf(sdf.filter("sindex=='SO53'"))
gdf0 = gdf[["fid", "score", "geometry_olf"]].groupby("fid").first().pipe(to_gdf, column="geometry_olf")
gdf1 = gdf[["id_parcel", "geometry"]].groupby("id_parcel").first()
gdf1.plot(ax=plot_gdf(gdf0, column="score", cmap="GnBu", linewidth=1, vmin=0, vmax=1), color="goldenrod", alpha=0.5, edgecolor="darkgoldenrod", linewidth=0.5)

# COMMAND ----------

gdf = to_gdf(sdf.filter("sindex=='NY85'"))
gdf0 = gdf[["fid", "score", "geometry_olf"]].groupby("fid").first().pipe(to_gdf, column="geometry_olf")
gdf1 = gdf[["id_parcel", "geometry"]].groupby("id_parcel").first()
gdf1.plot(ax=plot_gdf(gdf0, column="score", cmap="GnBu", linewidth=1, vmin=0, vmax=1), color="goldenrod", alpha=0.5, edgecolor="darkgoldenrod", linewidth=0.5)

# COMMAND ----------

# MAGIC %md
# MAGIC # TODO: use segments too

# COMMAND ----------

sdf = (
    spark.sql(
        """
        SELECT segment.*, olf.* EXCEPT(olf.geometry), olf.geometry AS geometry_olf
        FROM segment JOIN olf
        ON ST_Intersects(segment.geometry, olf.geometry)
    """
    )
    .groupby("id_parcel", "id_segment")
    .agg(
        F.collect_set("fid").alias("fids"),
        F.collect_set("catchment").alias("catchments"),
        F.collect_set("subcatchment").alias("subcatchments"),
        F.sum("score").alias("score"),
    )
)


sdf.display()
sdf.count()
