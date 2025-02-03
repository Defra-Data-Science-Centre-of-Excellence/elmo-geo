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

from pyspark.sql import functions as F

from elmo_geo import register
from elmo_geo.datasets import defra_priority_habitat_parcels, protected_areas_parcels, reference_parcels

register()


sdf = (
    reference_parcels.sdf()
    .select("id_parcel", "area_ha")
    # .join(
    #     wfm_parcels.select("id_parcel"),
    #     on="id_parcel",
    #     how="right",
    # )
    .join(
        (
            defra_priority_habitat_parcels.sdf()
            # Priority Habitats geometries have multiple habitats, they are split for each habitat_name, and unioned here.
            .groupby("id_parcel", "fid")
            .agg(
                F.expr("ARRAY_CONTAINS(COLLECT_LIST(habitat_name), 'Deciduous woodland') AS is_woodland"),
                F.expr("FIRST(proportion) AS proportion"),
            )
            .groupby("id_parcel", "is_woodland")
            # Priority Habitats shouldn't overlap, so we sum them, but clip to 1 because there will be rounding errors.
            .agg(F.expr("LEAST(1, SUM(proportion)) AS p_phi"))
        ),
        on="id_parcel",
        how="outer",
    )
    .join(
        protected_areas_parcels.sdf().selectExpr("id_parcel", "proportion_any AS p_pa"),
        on="id_parcel",
        how="outer",
    )
    .groupby("is_woodland")
    .agg(
        F.expr("ROUND(SUM(area_ha)) AS ha"),  # Sum of parcel areas
        F.expr("ROUND(SUM(area_ha * p_phi)) AS ha_phi"),  # Sum of parcel areas that is Priority Habitat
        F.expr("ROUND(SUM(area_ha * p_pa)) AS ha_pa"),  # Sum of parcel areas that is Protected Area
        F.expr("ROUND(SUM(area_ha * p_pa * p_phi)) AS ha_pa_phi"),  # Sum of parcel areas this is Protected Area and Priority Habitat
    )
)


sdf.display()
