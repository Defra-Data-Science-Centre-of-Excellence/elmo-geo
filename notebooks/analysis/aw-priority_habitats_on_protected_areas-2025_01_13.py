# Databricks notebook source
# MAGIC %md
# MAGIC # Priority Habitats (PHI) on Protected Area (PA)
# MAGIC
# MAGIC | | Parcels Area (ha) | PHI (ha) | PA (ha) | PHI and PA (ha) | PHI and PA (%) | PHI and not PA (ha) | PHI and not PA (%) |
# MAGIC |---|---|---|---|---|---|---|---|
# MAGIC | Woodland PHI | | 475,201 | | 78,371 | 16.5% | 396,830 | 83.5% |
# MAGIC | Non Woodland PHI | | 1,283,322 | | 614,482 | 47.9% | 668,840 | 52.1% |
# MAGIC | All | 9,780,226 | 1,758,523 | 1,087,113 | 692,853 | 39.4% | 1,065,670 | 60.6% |
# MAGIC

# COMMAND ----------

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from elmo_geo import register
from elmo_geo.datasets import (
    defra_priority_habitat_parcels,
    protected_areas_parcels,
    reference_parcels,
    rpa_hedges_raw,  # wfm_parcels,
)

register()

# COMMAND ----------

sdf = (
    reference_parcels.sdf()
    .select("id_parcel", "area_ha")
    # .join(
    #     wfm_parcels.select("id_parcel"),
    #     on="id_parcel",
    #     how="right",
    # )
    .join(
        SparkDataFrame.unionByName(
            rpa_hedges_raw.sdf().selectExpr(
                "CONCAT(sheet_id, parcel_ref) AS id_parcel",
                "'hedges' AS habitat_name",
                "ST_Length(geometry) AS m",
            ),
            defra_priority_habitat_parcels.sdf()
            .groupby(
                "id_parcel",
                "habitat_name",
            )
            .agg(
                F.expr("LEAST(1, SUM(proportion)) AS p_phi"),
            ),
            allowMissingColumns=True,
        ),
        on="id_parcel",
        how="outer",
    )
    .join(
        (
            protected_areas_parcels.sdf().selectExpr(
                "id_parcel",
                "proportion_any AS p_pa",
            )
        ),
        on="id_parcel",
        how="outer",
    )
    .selectExpr(
        "id_parcel",
        "habitat_name",
        "COALESCE(area_ha * p_phi, m) AS v_hab",
        "p_pa",
    )
    .groupby("habitat_name")
    .agg(
        F.expr("SUM(v_hab) AS v_hab"),
        F.expr("SUM(v_hab * p_pa) AS v_pa"),
    )
)


sdf.display()
