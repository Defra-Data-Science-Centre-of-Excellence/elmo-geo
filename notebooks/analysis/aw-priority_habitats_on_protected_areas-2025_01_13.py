# Databricks notebook source
# MAGIC %md
# MAGIC # Priority Habitats on Protected Area
# MAGIC
# MAGIC |                           | **ha**    | **ha_phi** | **ha_pa** | **ha_pa_phi** |
# MAGIC | ------------------------- | --------: | ---------: | --------: | ------------: |
# MAGIC | Assuming PA can overlap   | 9,780,226 |  1,758,930 |   728,992 |       674,134 |
# MAGIC | Assuming PA can't overlap | 9,780,226 |  1,758,930 |   777,959 |       714,887 |
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from elmo_geo import register
from elmo_geo.datasets import reference_parcels, protected_areas_parcels, defra_priority_habitat_parcels
register()



sdf = (
    reference_parcels.sdf().select("id_parcel", "area_ha")
    .join(
        (
            defra_priority_habitat_parcels.sdf()
            .groupby("id_parcel")
            # Priority Habitats shouldn't overlap, so we sum them, but clip to 1 because there will be rounding errors.
            .agg(F.expr("LEAST(1, SUM(proportion)) AS p_phi"))
        ),
        on="id_parcel",
        how="outer",
    )
    .join(
        (
            protected_areas_parcels.sdf()
            .unpivot(
                "id_parcel",
                ["proportion_sssi", "proportion_nnr", "proportion_sac", "proportion_spa", "proportion_ramsar", "proportion_mcz"],
                "protected_area",
                "proportion",
            )
            .groupby("id_parcel")
            # Protected Areas can overlap, so we calculate the probablistic area that is in any.
            .agg(F.expr("1 - EXP(SUM(LOG(1 - proportion))) AS p_pa"))
            # Assuming they don't overlap
            # .agg(F.expr("LEAST(1, SUM(proportion)) AS p_pa"))
        ),
        on="id_parcel",
        how="outer",
    )
    .selectExpr(
        "id_parcel",
        "area_ha",
        "COALESCE(p_phi, 0) AS p_phi",
        "COALESCE(p_pa, 0) AS p_pa",
    )
    .groupby()
    .agg(
        F.expr("SUM(area_ha) AS ha"),  # Sum of parcel areas
        F.expr("SUM(area_ha * p_phi) AS ha_phi"),  # Sum of parcel areas that is Priority Habitat
        F.expr("SUM(area_ha * p_pa) AS ha_pa"),  # Sum of parcel areas that is Protected Area
        F.expr("SUM(area_ha * p_pa * p_phi) AS ha_pa_phi"),  # Sum of parcel areas this is Protected Area and Priority Habitat
    )
)


sdf.display()
