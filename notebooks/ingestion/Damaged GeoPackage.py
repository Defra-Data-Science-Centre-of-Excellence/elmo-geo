# Databricks notebook source
# MAGIC %md
# MAGIC # Which GeoPackage files are damaged?
# MAGIC `files` is a list all the geopackages in `base` plus a demo working in `tmp`.
# MAGIC `test_gpkg_is_damaged` tries to read the metadata, this should take ~1 second for any file size.
# MAGIC
# MAGIC
# MAGIC I tried a timeout methodology but it doesn't seem to work with geopandas/pyogrio.
# MAGIC So manually cancelling is required.

# COMMAND ----------

import signal
from glob import glob
from multiprocessing import Pool
from random import shuffle

import pandas as pd
from pyogrio import read_info


class timeout:
    def __init__(self, seconds=3, error_message="Timeout"):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


def test_gpkg_is_damaged(filepath):
    with timeout():
        try:
            read_info(filepath)["features"]
            print(filepath)
        except Exception:
            pass


files = glob(
    "/dbfs/mnt/base/unrestricted/source_*/dataset_*/format_GPKG_*/**/*.gpkg", recursive=True
)
files.extend(glob("/dbfs/tmp/**/*.gpkg", recursive=True))  # Should always pass


# COMMAND ----------

success = """
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_community_forests/format_GPKG_community_forests/SNAPSHOT_2021_03_15_community_forests/refdata_owner.community_forest.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_england_boundary/format_GPKG_england_boundary/SNAPSHOT_2021_03_03_england_boundary/refdata_owner.eng_boundary.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_registered_common_land_bps_layer/format_GPKG_registered_common_land_bps_layer/SNAPSHOT_2021_03_05_registered_common_land_bps_layer/RCL.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_common_land_amalgamation/format_GPKG_common_land_amalgamation/SNAPSHOT_2023_02_07_common_land_amalgamation/refdata_owner.common_land_amalgamation.zip/refdata_owner.common_land_amalgamation/refdata_owner.common_land_amalgamation.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_common_land_amalgamation/format_GPKG_common_land_amalgamation/SNAPSHOT_2023_06_03_common_land_amalgamation/refdata_owner.common_land_amalgamation.zip/refdata_owner.common_land_amalgamation/refdata_owner.common_land_amalgamation.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_registered_common_land_bps_layer/format_GPKG_registered_common_land_bps_layer/SNAPSHOT_2023_02_07_registered_common_land_bps_layer/refdata_owner.rcl.zip/refdata_owner.rcl/refdata_owner.rcl.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_historic_common_land/format_GPKG_historic_common_land/LATEST_historic_common_land/common_land_historic.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_registered_common_land_bps_layer/format_GPKG_registered_common_land_bps_layer/LATEST_registered_common_land_bps_layer/refdata_owner.rcl.zip/refdata_owner.rcl/refdata_owner.rcl.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_historic_common_land/format_GPKG_historic_common_land/SNAPSHOT_2021_03_11_historic_common_land/common_land_historic.gpkg
/dbfs/tmp/awest/rpa-efa_hedge-2023_06_27.gpkg
/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_peaty_soils/format_GPKG_peaty_soils/LATEST_peaty_soils/refdata_owner.peaty_soils_location.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_community_forests/format_GPKG_community_forests/LATEST_community_forests/refdata_owner.community_forest.gpkg
/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_peaty_soils/format_GPKG_peaty_soils/SNAPSHOT_2021_03_24_peaty_soils/refdata_owner.peaty_soils_location.gpkg
/dbfs/mnt/base/unrestricted/source_natural_england_open_data_geoportal/dataset_nature_improvement_areas/format_GPKG_nature_improvement_areas/SNAPSHOT_2021_03_03_nature_improvement_areas/refdata_owner.nia.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_registered_common_land_bps_layer/format_GPKG_registered_common_land_bps_layer/SNAPSHOT_2023_06_03_registered_common_land_bps_layer/refdata_owner.rcl.zip/refdata_owner.rcl/refdata_owner.rcl.gpkg
/dbfs/mnt/base/unrestricted/source_natural_england_open_data_geoportal/dataset_nature_improvement_areas/format_GPKG_nature_improvement_areas/LATEST_nature_improvement_areas/refdata_owner.nia.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_community_forests/format_GPKG_community_forests/SNAPSHOT_2021_03_15_community_forests/refdata_owner.community_forest.gpkg
/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_peaty_soils/format_GPKG_peaty_soils/LATEST_peaty_soils/refdata_owner.peaty_soils_location.gpkg
/dbfs/mnt/base/unrestricted/source_natural_england_open_data_geoportal/dataset_nature_improvement_areas/format_GPKG_nature_improvement_areas/SNAPSHOT_2021_03_03_nature_improvement_areas/refdata_owner.nia.gpkg
/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_nitrate_vulnerable_zones_2017/format_GPKG_nitrate_vulnerable_zones_2017/LATEST_nitrate_vulnerable_zones_2017/refdata_owner.nvz_2017.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_common_land_amalgamation/format_GPKG_common_land_amalgamation/LATEST_common_land_amalgamation/refdata_owner.common_land_amalgamation.zip/refdata_owner.common_land_amalgamation/refdata_owner.common_land_amalgamation.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_common_land_amalgamation/format_GPKG_common_land_amalgamation/SNAPSHOT_2023_06_03_common_land_amalgamation/refdata_owner.common_land_amalgamation.zip/refdata_owner.common_land_amalgamation/refdata_owner.common_land_amalgamation.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_england_boundary/format_GPKG_england_boundary/SNAPSHOT_2021_03_03_england_boundary/refdata_owner.eng_boundary.gpkg
/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_peaty_soils/format_GPKG_peaty_soils/SNAPSHOT_2021_03_24_peaty_soils/refdata_owner.peaty_soils_location.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_community_forests/format_GPKG_community_forests/LATEST_community_forests/refdata_owner.community_forest.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_defra_parish_layer/format_GPKG_defra_parish_layer/SNAPSHOT_2023_02_07_defra_parish_layer/refdata_owner.parishes.zip/refdata_owner.parishes/refdata_owner.parishes.gpkg
/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_nitrate_vulnerable_zones_2017/format_GPKG_nitrate_vulnerable_zones_2017/SNAPSHOT_2021_03_03_nitrate_vulnerable_zones_2017/refdata_owner.nvz_2017.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_defra_parish_layer/format_GPKG_defra_parish_layer/LATEST_defra_parish_layer/refdata_owner.parishes.zip/refdata_owner.parishes/refdata_owner.parishes.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_england_boundary/format_GPKG_england_boundary/SNAPSHOT_2023_02_07_england_boundary/refdata_owner.england_boundary_line.zip/refdata_owner.england_boundary_line/refdata_owner.england_boundary_line.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_chrome_2021/format_GPKG_chrome_2021/LATEST_chrome_2021/crome_2021.zip/crome_2021.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_common_land_amalgamation/format_GPKG_common_land_amalgamation/SNAPSHOT_2023_02_07_common_land_amalgamation/refdata_owner.common_land_amalgamation.zip/refdata_owner.common_land_amalgamation/refdata_owner.common_land_amalgamation.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_historic_common_land/format_GPKG_historic_common_land/SNAPSHOT_2021_03_11_historic_common_land/common_land_historic.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_defra_parish_layer/format_GPKG_defra_parish_layer/SNAPSHOT_2021_03_03_defra_parish_layer/refdata_owner.parishes.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_lfa_and_moorland_line/format_GPKG_lfa_and_moorland_line/LATEST_lfa_and_moorland_line/refdata_owner.lfa.zip/refdata_owner.lfa/refdata_owner.lfa.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_chrome_2021/format_GPKG_chrome_2021/SNAPSHOT_2023_02_07_chrome_2021/crome_2021.zip/crome_2021.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_registered_common_land_bps_layer/format_GPKG_registered_common_land_bps_layer/LATEST_registered_common_land_bps_layer/refdata_owner.rcl.zip/refdata_owner.rcl/refdata_owner.rcl.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_registered_common_land_bps_layer/format_GPKG_registered_common_land_bps_layer/SNAPSHOT_2023_02_07_registered_common_land_bps_layer/refdata_owner.rcl.zip/refdata_owner.rcl/refdata_owner.rcl.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_lfa_and_moorland_line/format_GPKG_lfa_and_moorland_line/SNAPSHOT_2021_03_03_lfa_and_moorland_line/refdata_owner.lfa.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_environmental_stewardship_agreement_boundaries/format_GPKG_environmental_stewardship_agreement_boundaries/SNAPSHOT_2023_06_03_environmental_stewardship_agreement_boundaries/refdata_owner.es_agreement_ext.zip/refdata_owner.es_agreement_ext/refdata_owner.es_agreement_ext.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_registered_common_land_bps_layer/format_GPKG_registered_common_land_bps_layer/SNAPSHOT_2021_03_05_registered_common_land_bps_layer/RCL.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_registered_common_land_bps_layer/format_GPKG_registered_common_land_bps_layer/SNAPSHOT_2023_04_21_registered_common_land_bps_layer/refdata_owner.rcl.zip/refdata_owner.rcl/refdata_owner.rcl.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_lfa_and_moorland_line/format_GPKG_lfa_and_moorland_line/SNAPSHOT_2023_02_07_lfa_and_moorland_line/refdata_owner.lfa.zip/refdata_owner.lfa/refdata_owner.lfa.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_registered_common_land_bps_layer/format_GPKG_registered_common_land_bps_layer/SNAPSHOT_2023_06_03_registered_common_land_bps_layer/refdata_owner.rcl.zip/refdata_owner.rcl/refdata_owner.rcl.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_countryside_stewardship_agreement_boundaries/format_GPKG_countryside_stewardship_agreement_boundaries/SNAPSHOT_2021_03_16_countryside_stewardship_agreement_boundaries/cs_agreements.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_environmental_stewardship_agreement_boundaries/format_GPKG_environmental_stewardship_agreement_boundaries/SNAPSHOT_2023_03_09_environmental_stewardship_agreement_boundaries/refdata_owner.es_agreement_ext.zip/refdata_owner.es_agreement_ext/refdata_owner.es_agreement_ext.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_environmental_stewardship_agreement_boundaries/format_GPKG_environmental_stewardship_agreement_boundaries/SNAPSHOT_2023_06_27_environmental_stewardship_agreement_boundaries/refdata_owner.es_agreement_ext.zip/refdata_owner.es_agreement_ext/refdata_owner.es_agreement_ext.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_countryside_stewardship_agreement_boundaries/format_GPKG_countryside_stewardship_agreement_boundaries/SNAPSHOT_2023_06_27_countryside_stewardship_agreement_boundaries/refdata_owner.cs_agreement_ext.zip/refdata_owner.cs_agreement_ext/refdata_owner.cs_agreement_ext.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_countryside_stewardship_agreement_boundaries/format_GPKG_countryside_stewardship_agreement_boundaries/SNAPSHOT_2023_03_09_countryside_stewardship_agreement_boundaries/refdata_owner.cs_agreement_ext.zip/refdata_owner.cs_agreement_ext/refdata_owner.cs_agreement_ext.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_historic_common_land/format_GPKG_historic_common_land/LATEST_historic_common_land/common_land_historic.gpkg
/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_england_boundary/format_GPKG_england_boundary/LATEST_england_boundary/refdata_owner.england_boundary_line.zip/refdata_owner.england_boundary_line/refdata_owner.england_boundary_line.gpkg
/dbfs/tmp/awest/rpa-efa_hedge-2023_06_27.gpkg
""".split()[
    1:-1
]


success

# COMMAND ----------

# Run this, wait a little while, cancel, copy printed filepaths to success, repeat
todo = list(set(files).difference(set(success)))
shuffle(todo)
results = Pool().map(test_gpkg_is_damaged, todo)


results

# COMMAND ----------

df = pd.concat(
    [
        pd.DataFrame(
            {
                "filepath": list(set(success)),
                "is_damaged": False,
            }
        ),
        pd.DataFrame(
            {
                "filepath": list(set(todo)),
                "is_damaged": True,
            }
        ),
    ]
)

df.to_csv("/dbfs/tmp/damaged_gpkg_@3s.csv")
display(df)

# COMMAND ----------

df.query("is_damaged==True")["filepath"].str.split("/").str[5].unique().tolist(), df.query(
    "is_damaged==True"
)["filepath"].str.split("/").str[6].unique().tolist()
