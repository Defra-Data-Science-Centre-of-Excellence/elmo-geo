# Databricks notebook source
# MAGIC %md
# MAGIC # Priority Habitats on Protected Area
# MAGIC
# MAGIC |  | Parcel Area (ha)  | Parcel Area that is PHI (ha)  | Parcel Area that is PA (ha)  | Parcel Area that is both (ha)  | PHI on PA (%) | PA on PHI (%) |
# MAGIC |---|---|---|---|---|---|---|
# MAGIC | Not PHI | 4,773,744  | 0 | 8,748  | 0 |  |  |
# MAGIC | PHI | 6,116,808  | 1,758,523  | 1,059,380  | 673,838  | 38.3% | 63.6% |
# MAGIC | > PHI contains Deciduous Woodland | 3,591,679  | 475,201  | 362,556  | 77,272  | 16.3% | 21.3% |
# MAGIC | > PHI does not contains Deciduous Woodland | 2,525,129  | 1,283,322  | 696,824  | 596,566  | 46.5% | 85.6% |
# MAGIC | Total | 10,890,552  | 1,758,523  | 1,068,128  | 673,838  | 38.3% | 63.1% |

# COMMAND ----------

from pyspark.sql import functions as F

from elmo_geo import register
from elmo_geo.datasets import defra_priority_habitat_parcels, protected_areas_tidy_parcels, reference_parcels

register()


sdf = (
    reference_parcels.sdf()
    .select("id_parcel", "area_ha")
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
        (
            protected_areas_tidy_parcels.sdf()
            .withColumnRenamed("proportion", "p_pa")
        ),
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
