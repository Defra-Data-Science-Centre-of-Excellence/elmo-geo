"""Module for managing dataset information for processing vector geometries and intersecting them
with the land parcels dataset"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# TODO: could use pydantic here but would then need to pip install it each time which is a pain


@dataclass
class Dataset:
    """A class for holding dataset information for intersecting with land parcels
    Args:
        name: The name of the dataset (to be used in file naming)
        path_read: Path to the source data
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
    path_read: str
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
        if not self.path_read:
            raise ValueError("`path_read` must be defined")
        if self.path_polygons is None:
            self.path_polygons = f"/mnt/lab/unrestricted/elm/elmo/{self.name}/polygons.parquet"
        if self.path_output is None:
            self.path_output = f"/mnt/lab/unrestricted/elm/elmo/{self.name}/output.parquet"
        if self.keep_cols is None:
            self.keep_cols = []
        if self.rename_cols is None:
            self.rename_cols = {}
        if self.output_coltypes is None:
            self.output_coltypes = {}
        if self.read_kwargs is None:
            self.read_kwargs = {}


national_park = Dataset(
    name="national_park",
    path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/"
    "dataset_national_parks/format_SHP_national_parks/LATEST_national_parks/"
    "National_Parks_England.shp",
    keep_cols=["geometry", "name"],
    rename_cols={"name": "national_park_name"},
    output_coltypes={"national_park_name": "category"},
)

ramsar = Dataset(
    name="ramsar",
    path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_ramsar/"
    "format_SHP_ramsar/LATEST_ramsar/Ramsar_England.shp",
    keep_cols=["geometry", "name"],
    rename_cols={"NAME": "ramsar_name"},
)

peatland = Dataset(
    name="peatland",
    path_read="/dbfs/mnt/migrated-landing/Natural England/Peaty_Soils_Location__England_.geojson",
    keep_cols=["PCLASSDESC", "binary", "geometry"],
    rename_cols={"binary": "geometry", "PCLASSDESC": "peat_desc"},
    output_coltypes={"peat_desc": "category"},
)

national_character_areas = Dataset(
    name="national_character_areas",
    path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/"
    "dataset_national_character_areas/format_SHP_national_character_areas/"
    "LATEST_national_character_areas/National_Character_Areas___Natural_England.shp",
    keep_cols=["geometry", "NCA_Name"],
    rename_cols={"NCA_Name": "nca_name"},
)

sssi = Dataset(
    name="sssi",
    path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/"
    "dataset_sites_of_special_scientific_interest/"
    "format_SHP_sites_of_special_scientific_interest/LATEST_sites_of_special_scientific_interest/"
    "Sites_of_Special_Scientific_Interest_England.shp",
    keep_cols=["geometry", "sssi_name", "status"],
    rename_cols={"status": "sssi_status"},
    output_coltypes={"sssi_status": "category"},
)

aonb = Dataset(
    name="aonb",
    path_read="/dbfs/mnt/base/unrestricted/"
    "source_defra_data_services_platform/"
    "dataset_areas_of_outstanding_natural_beauty/"
    "format_SHP_areas_of_outstanding_natural_beauty/"
    "LATEST_areas_of_outstanding_natural_beauty/"
    "Areas_of_Outstanding_Natural_Beauty_England.shp",
    keep_cols=["geometry", "name"],
    rename_cols={"name": "aonb_name"},
)

lfa = Dataset(
    name="lfa",
    # path_read = "/dbfs/mnt/migrated-landing/General Access/lfa/refdata_owner.lfa.gpkg",
    path_read="/dbfs/mnt/lab/unrestricted/edward.burrows@defra.gov.uk/refdata_owner_lfa.gpkg",
    # path_read="/dbfs/mnt/lab/unrestricted/edward.burrows@defra.gov.uk/maglfa.shp"
    keep_cols=["geometry", "name"],
    rename_cols={"name": "lfa_type"},
    output_coltypes={"lfa_type": "category"},
    read_kwargs={"engine": "pyogrio"},
)

moorland = Dataset(
    name="moorland",
    path_read="/dbfs/mnt/lab/unrestricted/edward.burrows@defra.gov.uk/magmoor.shp",
    keep_cols=["geometry", "NAME"],
    rename_cols={"NAME": "moorland"},
    output_coltypes={"moorland": "category"},
)

region = Dataset(
    name="region",
    path_read="/dbfs/mnt/migrated-landing/Office of National Statistics/"
    "Regions__December_2021__EN_BFC.geojson",
    keep_cols=["geometry", "RGN21NM"],
    rename_cols={"RGN21NM": "region"},
    output_coltypes={"region": "category"},
)

commons = Dataset(
    name="commons",
    path_read="/dbfs/mnt/migrated-landing/General Access/CRoWAct2000Section4ConclRegCommonLand/SHP/"
    "Crow_Act_2000_Section_4_Conclusive_Registered_Common_Land_England.shp",
    keep_cols=["geometry", "cl_number", "name"],
    rename_cols={"cl_number": "common_number", "name": "common_name"},
)

flood_risk_areas = Dataset(
    name="flood_risk_areas",
    path_read="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/"
    "dataset_flood_risk_areas/format_SHP_flood_risk_areas/LATEST_flood_risk_areas/"
    "Flood_Risk_Areas.shp",
    keep_cols=["geometry", "flood_sour", "fra_name", "fra_id"],
    output_coltypes={"flood_sour": "category"},
)

ewco_red_squirrel = Dataset(
    name="ewco_red_squirrel",
    path_read="/dbfs/mnt/lab/unrestricted/elm/elmo/ewco_red_squirrel/"
    "EWCO_Biodiversity_-_Priority_Species_-_Red_Squirrel_-_Woodland_Creation.shp",
    keep_cols=["geometry", "status", "cswcm_pnts", "ewco_val", "sitename", "cat"],
    output_coltypes={
        "sitename": "category",
        "cat": "category",
        "cswcm_pnts": "int8",
        "ewco_val": "int8",
    },
)

ewco_priority_habitat_network = Dataset(
    name="ewco_priority_habitat_network",
    path_read="/dbfs/mnt/lab/unrestricted/elm/elmo/ewco_priority_habitat_network/"
    "EWCO_Biodiversity_Priority_Habitat_Network.shp",
    keep_cols=["geometry", "csht_pnts", "cswcm_pnts", "ewco_val", "cat"],
    output_coltypes={
        "cat": "category",
        "csht_pnts": "int8",
        "ewco_val": "int8",
    },
)

ewco_close_to_settlements = Dataset(
    name="ewco_close_to_settlements",
    path_read="/dbfs/mnt/lab/unrestricted/elm/elmo/ewco_close_to_settlements/EWCO_-_NfC_Social.shp",
    keep_cols=["geometry", "status"],
    output_coltypes={"status": "category"},
)

tiles = Dataset(
    name="tiles",
    path_read="/dbfs/mnt/lab/unrestricted/elm/elmo/baresoil/tiles.parquet",
    path_polygons="mnt/lab/unrestricted/elm/elmo/baresoil/tile_polygons.parquet",
    path_output="mnt/lab/unrestricted/elm/elmo/baresoil/parcel2023.parquet",
    keep_cols=["geometry", "Name"],
    rename_cols={"name": "tile"},
    output_coltypes={"tile": "string"},
)


datasets = [
    national_park,
    ramsar,
    peatland,
    national_character_areas,
    sssi,
    aonb,
    lfa,
    moorland,
    region,
    commons,
    flood_risk_areas,
    ewco_red_squirrel,
    ewco_priority_habitat_network,
    ewco_close_to_settlements,
    tiles,
]
"""A list of all defined datasets"""
