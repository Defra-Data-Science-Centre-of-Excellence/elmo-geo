# Databricks notebook source
# MAGIC %md
# MAGIC # Hedgerows on Protected Landscapes
# MAGIC
# MAGIC [deft-protected_landscapes_analysis-2025_01_27.csv](^dl_link)
# MAGIC
# MAGIC | source             | name                                   |   ha_pl | ha_parcels |   m_hedge |
# MAGIC | :----------------- | :------------------------------------- | ------: | ---------: | --------: |
# MAGIC | National Park      | Dartmoor                               |  96,401 |     72,839 | 6,725,804 |
# MAGIC | National Park      | Exmoor                                 |   9,058 |      6,701 |   357,743 |
# MAGIC | National Park      | Lake District                          |  26,913 |     18,763 | 1,722,072 |
# MAGIC | National Park      | Yorkshire Dales                        |  19,137 |     13,334 |   736,605 |
# MAGIC | National Park      | New Forest                             |  55,898 |     49,777 | 2,082,540 |
# MAGIC | National Park      | North York Moors                       |  44,591 |     29,096 | 1,218,270 |
# MAGIC | National Park      | Northumberland                         |  17,182 |     12,435 | 1,252,258 |
# MAGIC | National Park      | Peak District                          |  13,335 |      7,561 |   273,377 |
# MAGIC | National Park      | South Downs                            |   9,917 |      6,847 |   385,366 |
# MAGIC | National Park      | The Broads                             |  12,255 |      9,549 |   615,352 |
# MAGIC | National Landscape | Cornwall                               |  42,246 |     19,086 |   608,517 |
# MAGIC | National Landscape | Dedham Vale                            |  32,735 |     13,591 |   952,865 |
# MAGIC | National Landscape | East Devon                             |  95,574 |     80,181 | 4,375,196 |
# MAGIC | National Landscape | Isle Of Wight                          |  69,312 |     57,985 | 3,461,204 |
# MAGIC | National Landscape | Lincolnshire Wolds                     | 236,239 |    194,811 | 3,524,635 |
# MAGIC | National Landscape | Norfolk Coast                          | 218,484 |    204,391 | 1,737,630 |
# MAGIC | National Landscape | North Devon                            |   7,587 |      3,153 |   184,985 |
# MAGIC | National Landscape | Northumberland Coast                   |  36,959 |     28,655 | 2,829,568 |
# MAGIC | National Landscape | Quantock Hills                         |   6,866 |      2,919 |    82,823 |
# MAGIC | National Landscape | Solway Coast                           |   7,316 |      2,803 |   138,928 |
# MAGIC | National Landscape | Surrey Hills                           |  83,830 |     50,957 | 2,390,487 |
# MAGIC | National Landscape | Wye Valley                             | 204,108 |    157,324 | 8,382,849 |
# MAGIC | National Landscape | Arnside & Silverdale                   |  98,595 |     78,106 | 3,217,552 |
# MAGIC | National Landscape | Blackdown Hills                        | 112,933 |     88,232 | 5,448,081 |
# MAGIC | National Landscape | Cannock Chase                          |  80,573 |     70,694 | 1,638,508 |
# MAGIC | National Landscape | Chichester Harbour                     | 146,173 |     81,907 | 3,774,150 |
# MAGIC | National Landscape | Chilterns                              |  20,420 |     15,939 |   784,729 |
# MAGIC | National Landscape | Cotswolds                              |  16,831 |      1,259 |   113,908 |
# MAGIC | National Landscape | Cranborne Chase & West Wiltshire Downs |  87,900 |     55,888 | 2,181,068 |
# MAGIC | National Landscape | Dorset                                 |  10,664 |      6,902 |   414,812 |
# MAGIC | National Landscape | Forest Of Bowland                      |  19,847 |     14,721 | 1,029,495 |
# MAGIC | National Landscape | High Weald                             |  60,117 |     52,307 | 1,141,919 |
# MAGIC | National Landscape | Howardian Hills                        | 198,517 |    188,075 | 1,116,203 |
# MAGIC | National Landscape | Isles Of Scilly                        | 173,105 |    135,761 | 4,373,519 |
# MAGIC | National Landscape | Kent Downs                             |  80,829 |     66,231 | 4,163,233 |
# MAGIC | National Landscape | Malvern Hills                          |  33,973 |     24,357 | 2,601,554 |
# MAGIC | National Landscape | Mendip Hills                           |  44,349 |     25,936 |   803,214 |
# MAGIC | National Landscape | Nidderdale                             |  19,649 |     12,671 | 1,314,599 |
# MAGIC | National Landscape | North Pennines                         |  56,652 |     36,939 |   856,760 |
# MAGIC | National Landscape | North Wessex Downs                     | 144,106 |    107,807 | 3,172,064 |
# MAGIC | National Landscape | Shropshire Hills                       | 105,094 |     80,450 |   299,412 |
# MAGIC | National Landscape | South Devon                            | 143,783 |    124,470 | 1,763,591 |
# MAGIC | National Landscape | Suffolk Coast & Heaths                 | 165,268 |    113,130 | 4,204,435 |
# MAGIC | National Landscape | Tamar Valley                           |  30,188 |     21,074 |   240,233 |
# MAGIC
# MAGIC
# MAGIC [dl_link]: https://adb-7422054397937474.14.azuredatabricks.net/files/downloads/deft-protected_landscapes_analysis-2025_01_27.csv

# COMMAND ----------

from pandera import DataFrameModel, Field
from pyspark.sql import functions as F, DataFrame as SparkDataFrame

from elmo_geo import register
from elmo_geo.datasets import (
    protected_landscapes_tidy,
    protected_landscapes_parcels,
    rpa_hedges_raw,
    wfm_parcels,
)
from elmo_geo.io import download_link, load_sdf
from elmo_geo.st import sjoin
from elmo_geo.st.udf import st_clean
from elmo_geo.etl import Dataset, DerivedDataset
from elmo_geo.etl.transformations import _st_union_right
from elmo_geo.utils.misc import info_sdf


register()

# COMMAND ----------

sdf_hedges_on_pl = (
    sjoin(
        rpa_hedges_raw.sdf().select("geometry"),
        protected_landscapes_tidy.sdf().select("source", "name", "geometry"),
        lsuffix="_right",
        rsuffix="_left",
    )
    .selectExpr(
        "source",
        "name",
        "spark_partition_id() AS pid",
        "ST_AsBinary(geometry_left) AS geometry_left",
        "ST_AsBinary(geometry_right) AS geometry_right",
    )
    .transform(lambda sdf: sdf.groupby("source", "name", "pid").applyInPandas(_st_union_right, sdf.schema))  # This is to avoid OOM.
    .transform(lambda sdf: sdf.groupby("source", "name").applyInPandas(_st_union_right, sdf.schema))
    .selectExpr(
        "source",
        "name",
        """ST_Length(ST_Intersection(
            ST_CollectionExtract(ST_GeomFromWKB(geometry_left), 3),
            ST_CollectionExtract(ST_GeomFromWKB(geometry_right), 2)
        )) AS m""",
    )
)



sdf_hedges_on_pl.display()

# COMMAND ----------

sdf = (
    wfm_parcels.sdf()
    .join(
        protected_landscapes_parcels.sdf(),
        on = "id_parcel",
        how = "outer",
    )
    .withColumn("ha_parcels", F.expr("ha_parcel_geo * proportion"))
    .groupby("source", "name").agg(
        F.expr("SUM(ha_parcels) AS ha_parcels"),
    )
    .join(
        protected_landscapes_tidy.sdf(),
        on = ["source", "name"],
        how = "outer",
    )
    .join(
        sdf_hedges_on_pl,
        on = ["source", "name"],
        how = "outer",
    )
    .selectExpr(
        "source",
        "name",
        "ROUND(ST_Area(geometry) / 1e4) AS ha_pl",
        "ROUND(ha_parcels) AS ha_parcels",
        "ROUND(m) AS m_hedge",
    )
)


sdf.display()
f = "/dbfs/FileStore/downloads/deft-protected_landscapes_analysis-2025_01_27.csv"
sdf.toPandas().to_csv(f)
download_link(f)
