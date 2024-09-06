"""Ecological Site Classification M3 tree suitability and carbon sequestration outputs.

These datasets are the outputs of Forest Research's Ecolocial Site Classification (ESC)[^1]
tree suitability and yield class models. They provide suitability suitability scores for different
tree species under four Representative Concentration Pathway (RCP) climate scenarios and four
time periods. These outputs are reported for 1km Ordnance Survey GB grid cells.

This module also defines derived datasets that aggregate the 1km grid ESC trees outputs to RPA
parcels.

# Source dataset details

Full ESC scenaios documentation is available on SharePoint: [EVAST M3 Woodland Scenarios and Methods](https://defra.sharepoint.com/:w:/r/teams/Team1645/Evidence%20and%20Analysis%20WS/4.9_Workstream_Areas_Modelling_Strategy/4.9.7%20Modelling%20Strategy%20Documentation/Incoming/20240712%20-%20Amy%20Thomas%20-%20ESC%20tree%20data/ESC%20Trees%20Documentation%20Update%202024-07-31/EVAST%20M3%20Woodland%20Scenarios%20%26%20Method.docx?d=w48df3b0d3233438d8252a568155bf08a&csf=1&web=1&e=c5n9XP).
The following paraphrases this document.

There are two components to the outputs:
- tree suitability and yield scores (produce by ESC)
- carbon storage (produced by the CARBINE model)

Tree suitability and yeild class scores are modelled based on CHESS-SCAPE UKCP18 future climate projections
(RCP scenarios 2.6, 4.5, 6.0 and 8.5). A single score for each is given for each 20 time period from 1980-2000 to
2060-2080.

ESC suitability and yield class values for each species are used to simulate forest management scenarios, and model
tree and stand growth with CARBINE. There are four time periods over which change in carbon stored is modelled,
from 2021 to 2028, 2036, 2050, and 2100. The average species suitability and yield scores over the time period is used
for selecting the three most suitable/highest yielding species are at 1km grid for each forecast.

The woodland type scenario defines the long list of potential tree species (from which the three most suitable species
are chosen) and the woodland management prescription. This results in each woodland type, RCP scenario, time period and
1km grid being assigned three tree species, from which carbon sequestration is modelled. Woodland type and RCP categories
are split between source data files, with each file containing all potential time periods and 1km grids.

Carbon storage values are provided for two time periods. The whole time period from 2021, and the section
of the time period corresponding to a single sutiability and yield class score (either 2021-2028, 2029-2036,
2037-2051, or 2051-2100). Values are maximum potential carbon sequestration, negative values represent net
sequestration and positive values net emissions.

[^1] [Forest Research - Ecological Site Classification](https://www.forestresearch.gov.uk/tools-and-resources/fthr/ecological-site-classification)
"""

import itertools

from pandera import DataFrameModel, Field

from elmo_geo.etl import SourceDataset


class ESCM3WoodlandScenariosRaw(DataFrameModel):
    """Varaibles in the raw ESC M3 woodland scenario datasets.

    Attributes:
    X_BNG: OSGB Easting x-coordinate. (EPSG: 27700) lower left corner of grid cell
    Y_BNG: OSGB Northing y-coordinate. (EPSG: 27700) lower left corner of grid cell
    species_1: Tree species acronym - 1st most suitable or highest yielding, depending on selection criteria
    species_2: Tree species acronym - 2nd most suitable or highest yielding, depending on selection criteria
    species_3: Tree species acronym - 3rd most suitable or highest yielding, depending on selection criteria
    area_1: Percentage of 1km grid square allocated to planting of species 1
    area_2: Percentage of 1km grid square allocated to planting of species 2
    area_3: Percentage of 1km grid square allocated to planting of species 3
    open_space: Percentage of 1km grid square allocated to open space (no tree planting)
    yield_class_1: ESC yield class for species 1
    yield_class_2: ESC yield class for species 2
    yield_class_3: ESC yield class for species 3
    suitability_1: ESC suitability score for species 1
    suitability_2: ESC suitability score for species 2
    suitability_3: ESC suitability score for species 3
    period_AA_T1: Time periods for annual average (AA) and T1 carbon values
    period_AA_T1_duration: Number of years in each time period (AA_T1)
    AA_grass: Carbon stored in trees, litter, deadwood & soil, annual average over time period (period_AA_T1), previous land use grassland
    AA_crop: Carbon stored in trees, litter, deadwood & soil, annual average over time period (period_AA_T1), previous land use cropland
    AA_grass_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products,
    annual average over time period (period_AA_T1), previous land use grassland
    AA_crop_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products,
    annual average over time period (period_AA_T1), previous land use cropland
    T1_grass: Carbon stored in trees, litter, deadwood & soil, sum over time period (period_AA_T1), previous land use grassland
    T1_crop: Carbon stored in trees, litter, deadwood & soil, sum over time period (period_AA_T1), previous land use cropland
    T1_grass_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products, sum over time period (period_AA_T1), previous land use grassland
    T1_crop_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products, sum over time period (period_AA_T1), previous land use cropland
    period_T2: Time periods for T2 carbon values: 2021_2028, 2021_2036, 2021_2050, 2021_2100
    period_T2_duration: Number of years in each time period (T2): 8, 16, 30, 80
    T2_grass: Carbon stored in trees, litter, deadwood & soil, cumulative sum of T1 carbon values
    over time period (period_T2), previous land use grassland
    T2_crop: Carbon stored in trees, litter, deadwood & soil, cumulative sum of T1 carbon values
    over time period (period_T2), previous land use cropland
    T2_grass_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products,
    cumulative sum of T1 carbon values over time period (period_T2), previous land use grassland
    T2_crop_wood: Carbon stored in trees, litter, deadwood, soil & harvested wood products,
    # cumulative sum of T1 carbon values over time period (period_T2), previous land use cropland
    # tree_carbon: carbon sequestered into standing trees tCO2/ha average over period AA T1
    # litter_carbon: carbon sequestered into litter tCO2/ha average over period AA T1
    # deadwood_carbon: carbon sequestered into deadwood tCO2/ha average over period AA T1
    # grass_soil_carbon: carbon sequestered into soil (previously land use grassland) tCO2/ha average over period AA T1
    # crop_soil_carbon: carbon sequestered into soil (previous land use cropland) tCO2/ha average over period AA T1
    # wood_product_carbon_ipcc: carbon sequestered into wood products tCO2/ha average over period AA T1
    """

    X_BNG: int = Field()
    Y_BNG: int = Field()
    species_1: str = Field(nullable=True)
    species_2: str = Field(nullable=True)
    species_3: str = Field(nullable=True)
    area_1: float = Field(ge=0, le=1)
    area_2: float = Field(ge=0, le=1)
    area_3: float = Field(ge=0, le=1)
    open_space: float = Field(ge=0, le=1, coerce=True)
    yield_class_1: float = Field(nullable=True)
    yield_class_2: float = Field(nullable=True)
    yield_class_3: float = Field(nullable=True)
    suitability_1: float = Field(ge=0, le=1, nullable=True)
    suitability_2: float = Field(ge=0, le=1, nullable=True)
    suitability_3: float = Field(ge=0, le=1, nullable=True)
    period_AA_T1: str = Field()
    period_AA_T1_duration: int = Field()
    AA_grass: float = Field()
    AA_crop: float = Field()
    AA_grass_wood: float = Field()
    AA_crop_wood: float = Field()
    T1_grass: float = Field()
    T1_crop: float = Field()
    T1_grass_wood: float = Field()
    T1_crop_wood: float = Field()
    period_T2: str = Field()
    period_T2_duration: int = Field()
    T2_grass: float = Field()
    T2_crop: float = Field()
    T2_grass_wood: float = Field()
    T2_crop_wood: float = Field()
    tree_carbon: float = Field()
    litter_carbon: float = Field()
    deadwood_carbon: float = Field()
    grass_soil_carbon: float = Field()
    crop_soil_carbon: float = Field()
    wood_product_carbon_ipcc: float = Field(coerce=True)


source_dir = "/dbfs/mnt/lab/unrestricted/elm_data/evast/M3_trees_1km/"
esc_source_kwargs = {
    "level0": "bronze",
    "level1": "forest_research",
    "restricted": False,
    "is_geo": False,
    "model": ESCM3WoodlandScenariosRaw,
}

# Loop through combinations of woodland type and rcp scenario and create source datasets
woodland_types = ["native_broadleaved", "productive_conifer", "riparian", "silvoarable", "wood_pasture"]
rcps = ["26", "45", "60", "85"]
esc_source_datasets = []
for wt, rcp in itertools.product(woodland_types, rcps):
    source_dataset = SourceDataset(
        **esc_source_kwargs,
        name=f"esc_{wt}_rcp{rcp}_raw",
        source_path=source_dir + f"EVAST_M3_{wt}_rcp{rcp}.csv",
    )
    f"""ESC {wt.replace("_", " ")} dataset for the RCP {int(rcp)/10} scenario."""
    esc_source_datasets.append(source_dataset)

# unpack source datasets
(
    esc_native_broadleaved_26_raw,
    esc_native_broadleaved_45_raw,
    esc_native_broadleaved_60_raw,
    esc_native_broadleaved_85_raw,
    esc_productive_conifer_26_raw,
    esc_productive_conifer_45_raw,
    esc_productive_conifer_60_raw,
    esc_productive_conifer_85_raw,
    esc_riparian_26_raw,
    esc_riparian_45_raw,
    esc_riparian_60_raw,
    esc_riparian_85_raw,
    esc_silvoarable_26_raw,
    esc_silvoarable_45_raw,
    esc_silvoarable_60_raw,
    esc_silvoarable_85_raw,
    esc_wood_pasture_26_raw,
    esc_wood_pasture_45_raw,
    esc_wood_pasture_60_raw,
    esc_wood_pasture_85_raw,
) = esc_source_datasets
