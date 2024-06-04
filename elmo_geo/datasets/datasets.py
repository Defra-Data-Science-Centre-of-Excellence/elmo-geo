"""Module for managing dataset information for processing vector geometries and intersecting them
with the land parcels dataset
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# TODO: could use pydantic here but would then need to pip install it each time which is a pain


@dataclass
class Version:
    """A class for holding dataset information for intersecting with land parcels
    Args:
        name: The name of the version, usually date in the format `YYYY_MM_DD`
        path_read: Path to the source data
    """

    name: str
    path_read: str

    def __post_init__(self):
        if not self.name:
            raise ValueError("`name` must be defined")
        if not self.path_read:
            raise ValueError("`path_read` must be defined")


@dataclass
class Dataset:
    """A class for holding dataset information for intersecting with land parcels
    Args:
        name: The name of the dataset (to be used in file naming)
        source: The source of the dataset, e.g. `rpa`
        versions: Versions available - will be string formatted into path_read
        path_polygons: Path to save the intermediate cleaned data, defaults
            to `f"/mnt/lab/unrestricted/elmo/{self.name}/polygons.parquet"`
        path_output: Path to save the final cleaned data, defaults
            to `f"/mnt/lab/unrestricted/elmo/{self.name}/output.parquet"`
        keep_cols: List of columns to keep in the dataset, all other are dropped, defaults to `[]`
        rename_cols: Dictionary of columns to rename with their old and new values, defaults to `{}`
        output_coltypes: Dictionary of columns to change type of before feathering, useful to set
            strings to `"category"`, defaults to `{}`
        read_kwargs: Arguments to pass to geopandas for reading the data in e.g. specifying an
            engine
    """

    name: str
    source: str
    versions: List[Version]
    path_polygons: Optional[str] = None
    path_output: Optional[str] = None
    keep_cols: Optional[List[str]] = None
    rename_cols: Optional[Dict[str, str]] = None
    output_coltypes: Optional[Dict[str, str]] = None
    read_kwargs: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        """Set the default values where undefined"""
        if not self.name:
            raise ValueError("`name` must be defined")
        if not self.source:
            raise ValueError("`source` must be defined")
        if self.path_polygons is None:
            self.path_polygons = f"/mnt/lab/unrestricted/elm/{self.source}/{self.name}/{{version}}/polygons.parquet"
        if self.path_output is None:
            self.path_output = f"/mnt/lab/unrestricted/elm/{self.source}/{self.name}/{{version}}/output.parquet"
        if self.keep_cols is None:
            self.keep_cols = []
        if self.rename_cols is None:
            self.rename_cols = {}
        if self.output_coltypes is None:
            self.output_coltypes = {}
        if self.read_kwargs is None:
            self.read_kwargs = {}


parcels = Dataset(
    name="parcels",
    source="rpa",
    versions=[
        Version(
            name="2021_03_16",
            path_read="dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2021_03_16.parquet",
        ),
        Version(
            name="2021_11_16_adas",
            path_read="dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-adas.parquet",
        ),
        Version(
            name="2023_02_07",
            path_read="dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_02_07.parquet",
        ),
        Version(
            name="2023_06_03",
            path_read="dbfs:/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/2023_06_03.geoparquet",
        ),
        Version(
            name="2023_12_13",
            path_read="dbfs:/mnt/lab/restricted/ELM-Project/ods/rpa-parcel-2023_12_13.parquet",
        ),
    ],
)


alc = Dataset(
    name="alc",
    source="defra",
    versions=[
        Version(
            name="unknown",
            path_read="/dbfs/mnt/lab/unrestricted/elm_data/defra/alc/ALC_BMV_UNION_Merge_3b.shp",
        ),
    ],
    keep_cols=["geometry", "ALC_GRADE"],
    rename_cols={"ALC_GRADE": "alc"},
    output_coltypes={"alc": "category"},
)

national_park = Dataset(
    name="national_park",
    source="defra",
    versions=[
        Version(
            name="2021_03_15",
            path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_parks/format_SHP_national_parks/SNAPSHOT_2021_03_15_national_parks/National_Parks_England.shp",
        ),
        Version(
            name="2023_02_14",
            path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_parks/format_SHP_national_parks/SNAPSHOT_2023_02_14_national_parks/National_Parks_England.shp",
        ),
    ],
    keep_cols=["geometry", "name"],
    rename_cols={"name": "national_park_name"},
    output_coltypes={"national_park_name": "category"},
)


ramsar = Dataset(
    name="ramsar",
    source="defra",
    versions=[
        Version(
            name="2021_03_16",
            path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_ramsar/format_SHP_ramsar/SNAPSHOT_2021_03_16_ramsar/Ramsar_England.shp",
        ),
        Version(
            name="2023_02_14",
            path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_ramsar/format_SHP_ramsar/SNAPSHOT_2023_02_14_ramsar/Ramsar_England.shp",
        ),
    ],
    keep_cols=["geometry", "name"],
    rename_cols={"NAME": "ramsar_name"},
)


peaty_soils = Dataset(
    name="peaty_soils",
    source="defra",
    versions=[
        Version(
            name="2021_03_24",
            path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_peaty_soils/format_GPKG_peaty_soils/SNAPSHOT_2021_03_24_peaty_soils/refdata_owner.peaty_soils_location.gpkg",
        ),
    ],
    keep_cols=["pclassdesc", "geometry"],
    output_coltypes={"pclassdesc": "category"},
)

national_character_areas = Dataset(
    name="national_character_areas",
    source="defra",
    versions=[
        Version(
            name="2021_03_29",
            path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_national_character_areas/format_SHP_national_character_areas/SNAPSHOT_2021_03_29_national_character_areas/National_Character_Areas___Natural_England.shp",
        ),
    ],
    keep_cols=["geometry", "NCA_Name"],
    rename_cols={"NCA_Name": "nca_name"},
)

sssi = Dataset(
    name="sssi",
    source="defra",
    versions=[
        Version(
            name="2021_03_17",
            path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_sites_of_special_scientific_interest/format_SHP_sites_of_special_scientific_interest/SNAPSHOT_2021_03_17_sites_of_special_scientific_interest/Sites_of_Special_Scientific_Interest_England.shp",
        ),
        Version(
            name="2023_02_14",
            path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_sites_of_special_scientific_interest/format_SHP_sites_of_special_scientific_interest/SNAPSHOT_2023_02_14_sites_of_special_scientific_interest/Sites_of_Special_Scientific_Interest_England.shp",
        ),
    ],
    keep_cols=["geometry", "sssi_name", "status"],
    rename_cols={"status": "sssi_status"},
    output_coltypes={"sssi_status": "category"},
)
"""Proportional overlap with Sites of Special Scientific Interest (SSSI) with SSSI name. SSSIs can overlap so proportions may sum to >1."""

sssi_mask = Dataset(
    name="sssi_mask",
    source="defra",
    versions=[
        Version(
            name="2021_03_17",
            path_read=(
                "/dbfs/mnt/base/unrestricted/source_defra_data_services_platform"
                "/dataset_sites_of_special_scientific_interest"
                "/format_SHP_sites_of_special_scientific_interest"
                "/SNAPSHOT_2021_03_17_sites_of_special_scientific_interest"
                "/Sites_of_Special_Scientific_Interest_England.shp"
            ),
        ),
        Version(
            name="2023_02_14",
            path_read=(
                "/dbfs/mnt/base/unrestricted/source_defra_data_services_platform"
                "/dataset_sites_of_special_scientific_interest"
                "/format_SHP_sites_of_special_scientific_interest"
                "/SNAPSHOT_2023_02_14_sites_of_special_scientific_interest"
                "/Sites_of_Special_Scientific_Interest_England.shp"
            ),
        ),
    ],
    keep_cols=["geometry"],
)
"""Proportional overlap with any Sites of Special Scientific Interest (SSSI), no names."""

aonb = Dataset(
    name="aonb",
    source="defra",
    versions=[
        Version(
            name="2021_03_15",
            path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_areas_of_outstanding_natural_beauty/format_SHP_areas_of_outstanding_natural_beauty/SNAPSHOT_2021_03_15_areas_of_outstanding_natural_beauty/Areas_of_Outstanding_Natural_Beauty_England.shp",
        ),
        Version(
            name="2023_02_14",
            path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_areas_of_outstanding_natural_beauty/format_SHP_areas_of_outstanding_natural_beauty/SNAPSHOT_2023_02_14_areas_of_outstanding_natural_beauty/Areas_of_Outstanding_Natural_Beauty_England.shp",
        ),
    ],
    keep_cols=["geometry", "name"],
    rename_cols={"name": "aonb_name"},
)

lfa = Dataset(
    name="lfa",
    source="rpa",
    versions=[
        Version(
            name="2021_03_03",
            path_read="/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_lfa_and_moorland_line/format_GPKG_lfa_and_moorland_line/SNAPSHOT_2021_03_03_lfa_and_moorland_line/refdata_owner.lfa.gpkg",
        ),
        Version(
            name="2023_02_07",
            path_read="/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_lfa_and_moorland_line/format_GPKG_lfa_and_moorland_line/SNAPSHOT_2023_02_07_lfa_and_moorland_line/refdata_owner.lfa.zip/refdata_owner.lfa/refdata_owner.lfa.gpkg",
        ),
    ],
    keep_cols=["geometry", "name"],
    rename_cols={"name": "lfa_type"},
    output_coltypes={"lfa_type": "category"},
    read_kwargs={"engine": "pyogrio"},
)

region = Dataset(
    name="region",
    source="ons",
    versions=[
        Version(
            name="2021_12_01",
            path_read="/dbfs/mnt/migrated-landing/Office of National Statistics/Regions__December_2021__EN_BFC.geojson",
        ),
    ],
    keep_cols=["geometry", "RGN21NM"],
    rename_cols={"RGN21NM": "region"},
    output_coltypes={"region": "category"},
)

commons = Dataset(
    name="common_land",
    source="rpa",
    versions=[
        Version(
            name="2021_03_05",
            path_read="/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_registered_common_land_bps_layer/format_GPKG_registered_common_land_bps_layer/SNAPSHOT_2021_03_05_registered_common_land_bps_layer/RCL.gpkg",
        ),
        Version(
            name="2023_02_07",
            path_read="/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_registered_common_land_bps_layer/format_GPKG_registered_common_land_bps_layer/SNAPSHOT_2023_02_07_registered_common_land_bps_layer/refdata_owner.rcl.zip/refdata_owner.rcl/refdata_owner.rcl.gpkg",
        ),
    ],
    keep_cols=["geometry", "cl_number", "name"],
    rename_cols={"cl_number": "common_number", "name": "common_name"},
)

flood_risk_areas = Dataset(
    name="flood_risk_areas",
    source="defra",
    versions=[
        Version(
            name="2023_02_14",
            path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_flood_risk_areas/format_SHP_flood_risk_areas/SNAPSHOT_2023_02_14_flood_risk_areas/Flood_Risk_Areas.shp",
        ),
        Version(
            name="2021_03_18",
            path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_flood_risk_areas/format_SHP_flood_risk_areas/SNAPSHOT_2021_03_18_flood_risk_areas/Flood_Risk_Areas.shp",
        ),
    ],
    keep_cols=["geometry", "flood_sour", "fra_name", "fra_id"],
    output_coltypes={"flood_sour": "category"},
)

red_squirrel = Dataset(
    name="red_squirrel",
    source="ewco",
    versions=[
        Version(
            name="2022_10_18",
            path_read="/dbfs/mnt/lab/unrestricted/elm_data/ewco/red_squirrel/2022_10_18/EWCO_Biodiversity___Priority_Species___Red_Squirrel___Woodland_Creation.shp",
        ),
    ],
    keep_cols=["geometry", "status", "cswcm_pnts", "ewco_val", "sitename", "cat"],
    output_coltypes={
        "sitename": "category",
        "cat": "category",
        "cswcm_pnts": "int8",
        "ewco_val": "int8",
    },
)

priority_habitat_network = Dataset(
    name="priority_habitat_network",
    source="ewco",
    versions=[
        Version(
            name="2022_10_06",
            path_read="/dbfs/mnt/lab/unrestricted/elm_data/ewco/priority_habitat_network/2022_10_06/EWCO_Biodiversity___Priority_Habitat_Network.shp",
        ),
    ],
    keep_cols=["geometry", "csht_pnts", "cswcm_pnts", "ewco_val", "cat"],
    output_coltypes={
        "cat": "category",
        "csht_pnts": "int8",
        "ewco_val": "int8",
    },
)


priority_habitat_inventory = Dataset(
    name="priority_habitat_inventory",
    source="defra",
    versions=[
        Version(
            name="2021_03_26",
            path_read=("/dbfs/mnt/lab/unrestricted/elm_data/defra/priority_habitat_inventory/unified_2021_03_26.parquet"),
        ),
    ],
    keep_cols=["Main_Habit", "Confidence", "geometry"],
    output_coltypes={
        "Main_Habit": "category",
        "Confidence": "category",
        "proportion": "float",
    },
)

habitat_map = Dataset(
    name="habitat_map",
    source="living_england",
    versions=[
        Version(
            name="2022_09_16",
            path_read=("dbfs:/mnt/lab/unrestricted/elm_data/natural_england/living_england/2022_09_16.parquet"),
        ),
    ],
    keep_cols=["A_pred", "A_prob", "geometry"],
    output_coltypes={
        "proportion": "float",
        "A_pred": "category",
        "A_prob": "float",
    },
)

nfc_social = Dataset(
    name="nfc_social",
    source="ewco",
    versions=[
        Version(
            name="2022_03_14",
            path_read="/dbfs/mnt/lab/unrestricted/elm_data/ewco/nfc_social/2022_03_14/EWCO___NfC_Social.shp",
        ),
    ],
    keep_cols=["geometry", "status"],
    output_coltypes={"status": "category"},
)

water_quality = Dataset(
    name="water_quality",
    source="ewco",
    versions=[
        Version(
            name="2023_02_27",
            path_read="/dbfs/mnt/lab/unrestricted/elm_data/ewco/water_quality/2023_02_27/EWCO__E2_80_93_Water_Quality.shp",
        ),
    ],
    keep_cols=["geometry", "cat"],
    output_coltypes={"cat": "category"},
)

flood_risk_management = Dataset(
    name="flood_risk_management",
    source="ewco",
    versions=[
        Version(
            name="2023_02_24",
            path_read="/dbfs/mnt/lab/unrestricted/elm_data/ewco/flood_risk_management/2023_02_24/EWCO___Flood_Risk_Management.shp",
        ),
    ],
    keep_cols=["geometry", "LANDSCAPE"],
    rename_cols={"LANDSCAPE": "cat"},
    output_coltypes={"cat": "category"},
)

keeping_rivers_cool_riparian_buffers = Dataset(
    name="keeping_rivers_cool_riparian_buffers",
    source="ewco",
    versions=[
        Version(
            name="2023_03_03",
            path_read="/dbfs/mnt/lab/unrestricted/elm_data/ewco/keeping_rivers_cool_riparian_buffers/2023_03_03/EWCO___Keeping_Rivers_Cool_Riparian_Buffers.shp",
        ),
    ],
    keep_cols=["geometry", "OBJECTID"],
)

nfc_ammonia_emmissions = Dataset(
    name="nfc_ammonia_emmissions",
    source="ewco",
    versions=[
        Version(
            name="2022_03_14",
            path_read="/dbfs/mnt/lab/unrestricted/elm_data/ewco/nfc_ammonia_emmissions/2022_03_14/EWCO___NfC_Ammonia_Emissions_Capture_for_SSSI_Protection.shp",
        ),
    ],
    keep_cols=["geometry", "status", "pnts"],
    output_coltypes={"status": "category"},
)

tiles = Dataset(
    name="tiles",
    source="sentinel",
    versions=[
        Version(
            name="2023_02_07",
            path_read="/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/tiles.parquet",
        ),
    ],
    keep_cols=[
        "geometry",
        "Name",
    ],
    rename_cols={"Name": "tile"},
)

shine = Dataset(
    name="shine",
    source="historic_england",
    versions=[
        Version(
            name="2022_12_30",
            path_read="/dbfs/mnt/lab/restricted/ELM-Project/stg/he-shine-2022_12_30.parquet",
        ),
    ],
    keep_cols=[
        "geom",
        "shine_form",
    ],
    rename_cols={"geom": "geometry"},
)

scheduled_monuments = Dataset(
    name="scheduled_monuments",
    source="historic_england",
    versions=[
        Version(
            name="2024_04_29",
            path_read=(
                "/dbfs/mnt/base/unrestricted/source_historic_england_open_data_site/dataset_scheduled_monuments/format_GEOPARQUET_scheduled_monuments/SNAPSHOT_2024_04_29_scheduled_monuments/layer=Scheduled_Monuments.snappy.parquet"
            ),
        ),
    ],
    keep_cols=[
        "geometry",
        "datasets",
    ],
    rename_cols={"geom": "geometry"},
)

historic_archaeological = Dataset(
    name="historic_archaeological",
    source="historic_england",
    versions=[
        Version(
            # produced by the /notebooks/historic/01_combine_historic_datasets notebook
            name="2024_04_29",
            path_read=("/dbfs/mnt/lab/restricted/ELM-Project/stg/he-combined_sites-2024_05_03.parquet"),
        ),
    ],
    keep_cols=[
        "geometry",
        "datasets",
    ],
    rename_cols={"geom": "geometry"},
)

datasets = [
    alc,
    national_park,
    ramsar,
    peaty_soils,
    national_character_areas,
    sssi,
    sssi_mask,
    aonb,
    lfa,
    region,
    commons,
    flood_risk_areas,
    red_squirrel,
    priority_habitat_network,
    priority_habitat_inventory,
    habitat_map,
    nfc_social,
    water_quality,
    flood_risk_management,
    keeping_rivers_cool_riparian_buffers,
    nfc_ammonia_emmissions,
    tiles,
    shine,
    scheduled_monuments,
    historic_archaeological,
]
"""A list of all defined datasets"""
