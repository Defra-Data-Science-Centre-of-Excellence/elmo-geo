# Databricks notebook source
# MAGIC %md
# MAGIC # Moorwood
# MAGIC find moorland and woodland on parcels
# MAGIC
# MAGIC ### Data
# MAGIC - Forestry Commission, National Forest Inventory
# MAGIC - RPA, LFA and Moorland Line
# MAGIC - RPA, Parcels

# COMMAND ----------

import pyogrio
from pyspark.sql import functions as F

import elmo_geo
from elmo_geo.datasets.search import print_searchers
from elmo_geo.st import sjoin
from elmo_geo.utils.settings import FOLDER_ODS, FOLDER_STG

elmo_geo.register()

# COMMAND ----------

print_searchers("moor", "national_forest", "parcel", "peat")

# COMMAND ----------

datasets = {
    "rpa-moor-2021_03_03": "/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_lfa_and_moorland_line/format_GPKG_lfa_and_moorland_line/SNAPSHOT_201021_03_03_lfa_and_moorland_line/refdata_owner.lfa.gpkg",  # noqa:E501
    "fc-nfi-2020": "/dbfs/mnt/base/unrestricted/source_forestry_commission_open_data/dataset_national_forest_inventory_woodland_england_2020/format_SHP_national_forest_inventory_woodland_england_2020/SNAPSHOT_2022_10_19_national_forest_inventory_woodland_england_2020/National_Forest_Inventory_Woodland_England_2020.shp",  # noqa:E501
    # 'rpa-parcels-2023_09_07': '/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_reference_parcels/format_GPKG_rpa_reference_parcels/SNAPSHOT_2023_09_07_rpa_reference_parcels/reference_parcels.zip/reference_parcels.gpkg',  # noqa:E501 - Damaged
    "defra-nitrate_vulnerable_zones-2021_03_03": "/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_nitrate_vulnerable_zones_2017/format_GPKG_nitrate_vulnerable_zones_2017/SNAPSHOT_2021_03_03_nitrate_vulnerable_zones_2017/refdata_owner.nvz_2017.gpkg",  # noqa:E501
}

# COMMAND ----------

for name, f_in in datasets.items():
    for layer in pyogrio.list_layers(f_in)[:, 0]:
        f_stg = f"{FOLDER_STG}/{name}-{layer}.parquet"
        f_ods = f"{FOLDER_ODS}/{name}-{layer}.parquet"
        print(f"Staging:  {name}  {layer}")
        out = f"ogr2ogr -progress {f_stg} {f_in} {layer}"
        print(out)
    # break
    # gdf = pyogrio.read_dataframe(f_in, layer=layer).to_crs(27700)
    # gdf.to_parquet(f_stg)
    # print(f'Warehousing: {f_ods}')
    # sdf = spark.read.format('geoparquet').load(dbfs(f_stg, True))
    # to_gpq_partitioned(sdf, dbfs(f_ods, True))


# COMMAND ----------

# MAGIC %sh
# MAGIC IN='/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_rpa_reference_parcels/format_GPKG_rpa_reference_parcels/SNAPSHOT_2021_03_16_rpa_reference_parcels/reference_parcels.gpkg'  # noqa:E501
# MAGIC TMP='/databricks/driver/tmp.gpkg'
# MAGIC OUT='/dbfs/mnt/lab/restricted/ELM-Project/stg/rpa-parcels-2021_03_16-reference_parcels.parquet'
# MAGIC LAYER='reference_parcels'
# MAGIC
# MAGIC export PATH=$PATH:/databricks/miniconda/bin
# MAGIC
# MAGIC echo 'Temporary Copy'
# MAGIC ogr2ogr $TMP $IN
# MAGIC
# MAGIC echo 'Summary'
# MAGIC ogrinfo -so $TMP $LAYER
# MAGIC
# MAGIC echo 'Staging'
# MAGIC ogr2ogr $OUT $TMP
# MAGIC

# COMMAND ----------

sf_parcels_stg = "dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-parcels-2021_03_16-reference_parcels.parquet"
sf_parcels_ods = "dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcels-2021_03_16.parquet/"


sdf_parcels = (
    spark.read.format("geoparquet")
    .load(sf_parcels_stg)
    .select(
        F.concat("SHEET_ID", "PARCEL_ID").alias("id_parcel"),
        F.col("geom").alias("geometry"),
    )
)

sdf_parcels.write.format("geoparquet").save(sf_parcels_ods)
display(sdf_parcels)
sdf_parcels.count()

# COMMAND ----------

sf_moor_stg = "dbfs:/mnt/lab/restricted/ELM-Project/stg/rpa-moor-2021_03_03-refdata_owner_lfa_polygon.parquet"
sf_moor_ods = "dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-moor-2021_03_03.parquet/"


sdf_moor = (
    spark.read.format("geoparquet")
    .load(sf_moor_stg)
    .select(
        F.col("name").alias("class"),
        F.col("geom").alias("geometry"),
    )
)


sdf_moor.write.format("geoparquet").save(sf_moor_ods, mode="overwrite")
display(sdf_moor)

# COMMAND ----------

sf_nfi_stg = "dbfs:/mnt/lab/restricted/ELM-Project/stg/fc-nfi-2020-National_Forest_Inventory_Woodland_England_2020.parquet"
sf_nfi_ods = "dbfs:/mnt/lab/restricted/ELM-Project/stg/fc-nfi-2020.geoparquet"


sdf_nfi = (
    spark.read.format("geoparquet")
    .load(sf_nfi_stg)
    .select(
        F.concat("CATEGORY", F.lit(" - "), "IFT_IOA").alias("class"),
        "geometry",
    )
)


sdf_moor.write.format("geoparquet").save(sf_nfi_ods)
display(sdf_nfi)

# COMMAND ----------

sdf = (
    sdf_parcels.transform(sjoin, sdf_moor, how="full", lsuffix="", rsuffix="_moor")
    .transform(sjoin, sdf_nfi, how="full", lsuffix="", rsuffix="_nfi")
    .groupby("id_parcel")  # id_business
    .agg(
        F.collect_set("class_moor").alias("class_moor"),
        F.collect_set("class_nfi").alias("class_nfi"),
        F.expr("ST_Union_Aggr(geometry) AS geometry_parcel"),
        F.expr("ST_Union_Aggr(geometry_moor) AS geometry_moor"),
        F.expr("ST_Union_Aggr(geometry_nfi) AS geometry_nfi"),
    )
    .withColumn(
        "geometry_overlap",
        F.expr("ST_Intersection(geometry, ST_Intersection(geometry_moor, geometry_nfi))"),
    )
    # .select(
    #   # 'id_business',
    #   'id_parcel',
    #   'class_moor',
    #   'class_nfi',
    #   # F.expr('ST_Area(geometry)/10000 AS ha_parcel'),
    #   # F.expr('ST_Area(geometry_moor)/10000 AS ha_moor'),
    #   # F.expr('ST_Area(geometry_nfi)/10000 AS ha_nfi'),
    #   # F.expr('ST_Area(geometry_overlap)/10000 AS ha_overlap'),
    # )
)


display(sdf)

# COMMAND ----------

sdf.select().groupby().sum()


# COMMAND ----------

#                 k, n, p, pest
# total
# organic
# arable
# not arable
# list of outliers
