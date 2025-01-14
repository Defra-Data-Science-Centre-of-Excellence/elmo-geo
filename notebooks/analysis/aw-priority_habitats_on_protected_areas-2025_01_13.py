# Databricks notebook source
# MAGIC %md
# MAGIC # Priority Habitats on Protected Area
# MAGIC
# MAGIC |                           | **ha**    | **ha_phi** | **ha_pa** | **ha_pa_phi** |
# MAGIC | ------------------------- | --------: | ---------: | --------: | ------------: |
# MAGIC | Assuming PA can overlap   | 9,780,226 |  1,758,497 |   728,992 |       673,833 |
# MAGIC | Assuming PA can't overlap | 9,780,226 |  1,758,497 |   777,959 |       714,573 |
# MAGIC
# MAGIC Table 1: PHI vs PA.
# MAGIC
# MAGIC
# MAGIC |is_woodland|ha|ha_phi|ha_pa|ha_pa_phi|
# MAGIC |---|---|---|---|---|
# MAGIC |null|4,773,744|0|8,748|0|
# MAGIC |true|3,689,721|486,425|363,435|77,335|
# MAGIC |false|2,397,955|1,272,098|696,388|596,503|
# MAGIC
# MAGIC Table 2: PHI separated by Woodland.  This table assumes PA can overlap
# MAGIC
# MAGIC
# MAGIC | habitat_name                                    | is_woodland |
# MAGIC |-------------------------------------------------|-------------|
# MAGIC | Deciduous woodland                              | True        |
# MAGIC | Fragmented heath                                | False       |
# MAGIC | Coastal sand dunes                              | False       |
# MAGIC | Saline lagoons                                  | False       |
# MAGIC | Upland flushes fens and swamps                  | False       |
# MAGIC | Purple moor grass and rush pastures             | False       |
# MAGIC | Coastal vegetated shingle                       | False       |
# MAGIC | Lowland dry acid grassland                      | False       |
# MAGIC | Good quality semi improved grassland            | False       |
# MAGIC | Lowland meadows                                 | False       |
# MAGIC | Lowland fens                                    | False       |
# MAGIC | Ponds                                           | False       |
# MAGIC | Coastal saltmarsh                               | False       |
# MAGIC | Grass moorland                                  | False       |
# MAGIC | Limestone pavement                              | False       |
# MAGIC | Lowland calcareous grassland                    | False       |
# MAGIC | Mountain heaths and willow scrub                | False       |
# MAGIC | Lowland raised bog                              | False       |
# MAGIC | Upland heathland                                | False       |
# MAGIC | Reedbeds                                        | False       |
# MAGIC | No main habitat but additional habitats present | False       |
# MAGIC | Maritime cliff and slope                        | False       |
# MAGIC | Lowland heathland                               | False       |
# MAGIC | Upland calcareous grassland                     | False       |
# MAGIC | Calaminarian grassland                          | False       |
# MAGIC | Blanket bog                                     | False       |
# MAGIC | Lakes                                           | False       |
# MAGIC | Traditional orchard                             | True        |
# MAGIC | Upland hay meadow                               | False       |
# MAGIC | Mudflats                                        | False       |
# MAGIC | Coastal and floodplain grazing marsh            | False       |
# MAGIC
# MAGIC Table 3: Is woodland priority habitat.

# COMMAND ----------

from pyspark.sql import functions as F
from elmo_geo import register
from elmo_geo.datasets import reference_parcels, protected_areas_parcels, defra_priority_habitat_parcels
register()


woodland_phis = (
    'Deciduous woodland',
    'Traditional orchard',
)


sdf = (
    reference_parcels.sdf().select("id_parcel", "area_ha")
    .join(
        (
            defra_priority_habitat_parcels.sdf()
            # Priority Habitats geometries have multiple habitats, they are split for each habitat_name, and unioned here.
            .groupby("id_parcel", "fid")
            .agg(
                F.expr(f"ARRAYS_OVERLAP(COLLECT_LIST(habitat_name), ARRAY{woodland_phis}) AS is_woodland"),
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
            #.agg(F.expr("LEAST(1, SUM(proportion)) AS p_pa"))
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
