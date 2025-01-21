# Databricks notebook source
# MAGIC %md
# MAGIC # Priority Habitats on Protected Area
# MAGIC
# MAGIC |  | Parcels Area (ha) | Priority Habitat (ha) | Protected Area (ha) | Priority Habitat and Protected Area (ha) | Priority Habitat and Protected Area (%) | Priority Habitat and not Protected Area (ha) | Priority Habitat and not Protected Area (%) |
# MAGIC |---|---|---|---|---|---|---|---|
# MAGIC | Woodland Habitat |  | 475,201 |  | 77,272 | 16.3% | 397,929 | 83.7% |
# MAGIC | Non Woodland Habitat |  | 1,283,322 |  | 596,566 | 46.5% | 686,756 | 53.5% |
# MAGIC | All | 9,780,226 | 1,758,523 | 1,068,128 | 673,838 | 38.3% | 1,084,685 | 61.7% |

# COMMAND ----------

from pyspark.sql import functions as F

from elmo_geo import register
from elmo_geo.datasets import defra_priority_habitat_parcels, protected_areas_parcels, reference_parcels, wfm_parcels

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
