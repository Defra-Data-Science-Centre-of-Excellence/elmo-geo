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
from elmo_geo.io import download_link, load_sdf, to_gdf
from elmo_geo.plot.base_map import plot_gdf

register()


f_parcel = "/dbfs/mnt/lab/restricted/ELM-Project/silver/rpa-parcel-adas.parquet"
f_segment = "/dbfs/mnt/lab/restricted/ELM-Project/silver/elmo_geo-boundary_segment-2024_06_21.parquet"
f_olf = "/dbfs/mnt/lab/restricted/ELM-Project/bronze/ea-overland_flow-2024_06_19_direct.parquet"

f = "/dbfs/mnt/lab/restricted/ELM-Project/silver/environment_agency/elmo_geo-parcel_olf-2024_07_11_beta.parquet"

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
    "MaxFlowAcc",
    # "MajLandUse", "MeanSlope", "Slope1haWatershed", "MinFlowWater", "MinFlowRoad", "SSSI_Intersect", "FlowAccClass", "MaxFlowAcc",
    "NTILE(5) OVER (ORDER BY MaxFlowAcc) AS MaxFlowAccQuintile",  # Replace *MaxFlowAcc with +Quintile(MaxFlowAcc)
    "(CatchmentScore + LandUseScore + SlopeScore + CombinedSoilScore + MeanRainfallScore + MaxFlowAccQuintile) /6 AS CombinedScore",
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

# First group by parcels and olf objects to get single unique olf object per parcel, then group by parcels to get average score per parcel
pdf = (
    sdf.groupby("id_parcel", "fid")
    .agg(
        F.first("fid").alias("olf_fid"),
        F.first("CatchmentScore").alias("CatchmentScore"),
        F.first("LandUseScore").alias("LandUseScore"),
        F.first("SlopeScore").alias("SlopeScore"),
        F.first("SoilErosionScore").alias("SoilErosionScore"),
        F.first("SoilRunoffScore").alias("SoilRunoffScore"),
        F.first("CombinedSoilScore").alias("CombinedSoilScore"),
        F.first("ReceptorDistanceScore").alias("ReceptorDistanceScore"),
        F.first("MeanRainfallScore").alias("MeanRainfallScore"),
        F.first("MaxFlowAcc").alias("MaxFlowAcc"),
        F.first("MaxFlowAccQuintile").alias("MaxFlowAccQuintile"),
        F.first("CombinedScore").alias("CombinedScore"),
    )
    .groupby("id_parcel")
    .agg(
        F.count("olf_fid").alias("n_olf_fids"),
        F.array_join(F.collect_list("olf_fid"), delimiter=", ").alias("olf_fids"),
        F.mean("CatchmentScore").alias("CatchmentScore"),
        F.mean("LandUseScore").alias("LandUseScore"),
        F.mean("SlopeScore").alias("SlopeScore"),
        F.mean("SoilErosionScore").alias("SoilErosionScore"),
        F.mean("SoilRunoffScore").alias("SoilRunoffScore"),
        F.mean("CombinedSoilScore").alias("CombinedSoilScore"),
        F.mean("ReceptorDistanceScore").alias("ReceptorDistanceScore"),
        F.mean("MeanRainfallScore").alias("MeanRainfallScore"),
        F.mean("MaxFlowAcc").alias("MaxFlowAcc"),
        F.mean("CombinedScore").alias("CombinedScore"),
    )
    .withColumn("MaxFlowAccQuintileParcels", F.expr("NTILE(5) OVER (Order BY MaxFlowAcc)"))
    .drop("geometry", "geometry_olf")
    .toPandas()
)
pdf.to_parquet(f)
download_link(f)

# COMMAND ----------

assert any(pdf.id_parcel.duplicated()) is False

# COMMAND ----------

pdf.head()

# COMMAND ----------

gdf = to_gdf(sdf.filter("sindex=='SO53'"))
gdf0 = gdf[["fid", "CombinedScore", "geometry_olf"]].groupby("fid").first().pipe(to_gdf, column="geometry_olf")
gdf1 = gdf[["id_parcel", "geometry"]].groupby("id_parcel").first()
gdf1.plot(
    ax=plot_gdf(gdf0, column="CombinedScore", cmap="GnBu", linewidth=1, vmin=0, vmax=1),
    color="goldenrod",
    alpha=0.5,
    edgecolor="darkgoldenrod",
    linewidth=0.5,
)

# COMMAND ----------

gdf = to_gdf(sdf.filter("sindex=='NY85'"))
gdf0 = gdf[["fid", "CombinedScore", "geometry_olf"]].groupby("fid").first().pipe(to_gdf, column="geometry_olf")
gdf1 = gdf[["id_parcel", "geometry"]].groupby("id_parcel").first()
gdf1.plot(
    ax=plot_gdf(gdf0, column="CombinedScore", cmap="GnBu", linewidth=1, vmin=0, vmax=1),
    color="goldenrod",
    alpha=0.5,
    edgecolor="darkgoldenrod",
    linewidth=0.5,
)

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
