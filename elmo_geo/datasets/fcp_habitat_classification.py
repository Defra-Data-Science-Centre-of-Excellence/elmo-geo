"""Parcel level habitat creation and management classifications.

Habitat creation ELM actions require specifying the most suitable habitats to
create on a parcel. Similarly, habitat management actions require specifying which priority habitats already exist on each
parcel.

In both cases the habitat type assigned needs to be one of the following used by EVAST, grouped by the habitat creation/
management action.

|action                        |action_habitat       | habitat name |
| ---------------------------- | ------------------- |
|Create Wetland                | fen                 | Fen |
|Create Wetland                | bog                 | Bog|
|Create Species-rich Grassland | lowland_meadow      | Lowland Meadow|
|Create Species-rich Grassland | upland_meadow       | Upland Meadow |
|Create Species-rich Grassland | lowland_dry_acid_gr | Lowland Dry Acid Grassland |
|Create Species-rich Grassland | lowland_calc_gr     | Lowland calcareous grassland |
|Create Species-rich Grassland | lowland_acid_gr     | Lowland acid grassland |
|Create Species-rich Grassland | upland_acid_gr      | Upland acid grassland |
|Create Species-rich Grassland | upland_calc_gr      | Upland calcareous grassland |
|Create Heathland              | lowland             | Lowland heathland |
|Create Heathland              | upland              | Upland heathland


Currently the assignment process uses Soilscapes (with additional habitat lookup) and Priority Habitat Inventory datasets.
The habitat categories in these datasets do not map to all of the required habitat types listed above. The 'upland_acid_gr',
'lowland_acid_gr' and 'upland_meadow' species-rich grassland habitat types are not represented in these datasets. Additionally,
only the 'fen' type wetland habitat is assigned to parcels, with no parcels being assigned the 'bog' type habitat.

To resolved this issues, the CEH Land Cover Map dataset needs to be integrated into the assignment process.
# TODO: https://github.com/Defra-Data-Science-Centre-of-Excellence/elm_modelling_strategy/issues/887
"""

from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pyspark.sql import Window
from pyspark.sql import functions as F

from elmo_geo.etl import DerivedDataset, SourceDataset
from elmo_geo.utils.types import SparkDataFrame

from .cec_soilscapes import cec_soilscapes_habitats_parcels
from .defra_priority_habitats import defra_habitat_area_parcels, defra_priority_habitat_parcels
from .moor import is_upland_parcels
from .rpa_reference_parcels import reference_parcels


class EVASTHabitatsMappingModel(DataFrameModel):
    """EVAST's mapping between different habitat classification model.

    Parameters:
       action_group: The type of habitat creation action the habitats correspond to. Is either "Create Heathland",
            "Create Wetland", "Create SRG". SRG stands for species-rich grassland.
       action_habitat: The habitat type created by the habitat creation action. Referred to as the 'factor_level' by EVAST.
       bimla_habitat: Habitat type used by EVASTs BIMLA model.
       is_upland: Boolean indicating whether the habitat type is above the moorland line (True)
            or not (False) or can be both (None).
       habitat_name: Corresponding habitat name in alternative source.
       habitat_code_soilscapes: Corresponding habitat code in alternative source (only soilscapes habitats have a code).
       source: The alternative habitat dataset that is being mapped to the create habitat action habitat types. Either
            soilscapes, phi (priority habitat inventory), lcm (CEH land cover map), or aes (from agri-environment scheme
            uptake data).
    """

    action_group: Category = Field(coerce=True, isin=["Create Heathland", "Create Wetland", "Create SRG"])
    action_habitat: Category = Field(
        coerce=True,
        isin=[
            "upland",
            "lowland",
            "fen",
            "bog",
            "upland_acid_gr",
            "lowland_acid_gr",
            "lowland_dry_acid_gr",
            "lowland_calc_gr",
            "lowland_meadow",
            "upland_calc_gr",
            "upland_meadow",
        ],
    )
    is_upland: bool = Field(coerce=True, nullable=True)
    bimla_habitat: Category = Field(
        alias="BIMLA_model_grouping",
        coerce=True,
        isin=[
            "cvr_upland_heathland",
            "cvr_lowland_heathland",
            "cvr_bog",
            "cvr_fen",
            "cvr_lowland_calc_gr",
            "cvr_lowland_semi_natural",
            "cvr_upland_semi_nat_gr",
        ],
    )
    habitat_name: str = Field(nullable=True)
    habitat_code: str = Field(nullable=True)
    source: str = Field(nullable=True)


evast_habitat_mapping_raw = SourceDataset(
    name="evast_habitat_mapping_raw",
    level0="bronze",
    level1="evast",
    restricted=False,
    is_geo=False,
    model=EVASTHabitatsMappingModel,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/evast/EVAST_HabitatStocking_2024_08_29_M2_habitat_create_Classificatn.csv",
)
"""EVAST's mapping between different habitat classification model.

Used for the decision hierarch which classifies which habitats
can be created on parcels under habitat creation actions. This
mapping matches up habitat types under different datasets to the
habitat creation groups (Create Wetland, Create Species-rich
Grassland, Create Heathland) and sub-groups within these.

This dataset was provided in the ''M2 habitat create Classificatn'
tab of EVASTs [Habitat Stocking]:https://defra.sharepoint.com/:x:/r/teams/Team1645/_layouts/15/Doc.aspx?sourcedoc=%7B305701D9-E8E2-424B-8B3F-837B876133B6%7D&file=EVAST_HabitatStocking-2024_08_29.xlsx
workbook but has been reformatted to a 'long' format to align with the analysis pipeline used here.
"""


def _get_parcel_candidate_habitats(
    is_upland_parcels: DerivedDataset,
    cec_soilscapes_habitats_parcels: DerivedDataset,
    evast_habitat_mapping_raw: DerivedDataset,
) -> SparkDataFrame:
    """Identify candidate habitats that could be created on each parcel.

    Use soilscapes soil type and moorland line (upland/lowland classification)
    to get candidate habitat types for each parcel.
    """

    sdf_ss = cec_soilscapes_habitats_parcels.sdf().filter(F.expr("proportion>0.1")).select("id_parcel", "unit", "habitat_code", "habitat_type")

    # select lookup to action habitats for soilscapes habitats
    sdf_habitat_lu = (
        evast_habitat_mapping_raw.sdf()
        .filter(F.expr("source = 'soilscapes'"))
        .selectExpr("action_group", "action_habitat", "is_upland as is_upland_lu", "habitat_code")
    )

    # idenfity candidate habitats that can be created on parcels based on the parcel soil type
    # and whether the parcel is upland or lowland.
    return (
        is_upland_parcels.sdf()
        .join(sdf_ss, on="id_parcel", how="left")
        .join(sdf_habitat_lu, on="habitat_code", how="left")
        .filter(F.expr("(is_upland_lu is NULL) OR (is_upland=is_upland_lu)"))
        .select("id_parcel", "action_group", "action_habitat", "is_upland")
        .dropDuplicates()
    )


def _filter_candidates_by_phi(
    sdf_candidates: SparkDataFrame, defra_habitat_area_parcels: DerivedDataset, evast_habitat_mapping_raw: DerivedDataset, threshold_distances=[1_000, 3_000]
) -> SparkDataFrame:
    """Join candidate parcel habitat types to a dataset of nearby priority habitats
    in order to select which action habitat to apply to that parcel.

    Biases assignment of rare habitats where they are founds within the minimum threshold distance (1km by deafault). This
    is because rare habitats will typically have a smaller area and so could be underassigned by an area based assignment.

    The assignments is based on ranking habitats by threshold distance, the 'nearby_rare_habitat' flag, and PHI habitat area. This
    approximates a distance weighted area based assignment, with priority for rare habitats that are found within the minimum distance threshold.

    Parameters:
        sdf_candidates: Lookup from parcel ID to candidate habitats to assign to that parcel.
        defra_habitat_area_parcels: Dataset of area of priority habitats withing different threshold
            distances from each parcel.
        evast_habitat_mapping_raw: Lookup between habitat names used in the priority habitats inventory (PHI)
            and EVAST.
    """
    rare_habitats = [
        "Lowland Raised Bog",
        "Upland Fen",
        "Purple Moor Grass & Rush Pasture",
        "Lowland dry acid grassland",
        "Upland calcareous grassland",
        "Upland hay meadows",
    ]

    # Get habitat lookup for phi habitat names.
    # Create boolean nearby_rare_habitats to bias assignment of rare habitats.
    sdf_phi_lu = (
        evast_habitat_mapping_raw.sdf()
        .filter(F.expr("source = 'phi'"))
        .selectExpr("action_group", "action_habitat", "habitat_name")
        .dropDuplicates()
        .join(defra_habitat_area_parcels.sdf().filter(F.col("distance_threshold").isin(threshold_distances)), on="habitat_name", how="left")
        .withColumn("nearby_rare_habitat", F.col("habitat_name").isin(rare_habitats) & (F.col("distance_threshold") == min(threshold_distances)))
    )

    # Join nearby PHI habitats to the parcels. This links each parcel to a multiple soilscapes habitats and PHI habitats.
    # Filter this down to where there is aggrement between the soilscapes habitat and the PHI habitat by inner joining to the habitat lookup.
    # Finally, select one habitat per action group by ranking on distance and area.
    window = Window.partitionBy("id_parcel", "action_group").orderBy(
        F.col("distance_threshold").asc(), F.col("nearby_rare_habitat").desc(), F.col("area").desc()
    )
    return (
        sdf_candidates.join(sdf_phi_lu, on=["id_parcel", "action_group", "action_habitat"], how="inner")
        .withColumn("row_number", F.row_number().over(window))
        .filter("row_number=1")
        .select("id_parcel", "action_group", "action_habitat")
    )


def _habitat_creation_classification(
    is_upland_parcels: DerivedDataset,
    cec_soilscapes_habitats_parcels: DerivedDataset,
    evast_habitat_mapping_raw: DerivedDataset,
    defra_habitat_area_parcels: DerivedDataset,
):
    """Assign a habitat creation habitat type to each parcel.

    For each habitat creation action, create wetland, create species-rich grassland, create heathland,
    this function assigns parcels with the habitat type that will be created.

    The assignment works as follows:

    1. Based on the Natmap Scoilscape soil type of the parcel identify which habitats can potentially occur on the parcel.
    2. Filter this list to only include candidate habitats which correspond to the upland/lowland classification of the parcel.
    These are termed the 'candidate' habitats.
    3. Further filter the candidates to only assign habitats where an instance of the habitat exists within a threshold distance
    of the parcel according to the Priority Habitat Inventory (PHI). Where multiple such candidates exist assign the habitat with the
    largest area in the PHI. Rare habitats are prioristised if they are found within the minimum threshold distance of a parcel.

    Rare habitats (from EVAST):
    - Lowland Raised Bog
    - Upland Fen
    - Purple Moor Grass & Rush Pasture
    - Lowland dry acid grassland
    - Upland calcareous grassland
    - Upland hay meadows

    The threshold distance used depends on the habitat type. The function first tries to assign habitat types using a 1km distance
    threshold and then a 3km distance threshold. This is to prioritise nearby habitats where they match to the soilscapes habitat
    type, but permit a larger distance threshold where no match is found nearby.
    """

    return (
        _get_parcel_candidate_habitats(
            is_upland_parcels,
            cec_soilscapes_habitats_parcels,
            evast_habitat_mapping_raw,
        )
        .transform(_filter_candidates_by_phi, defra_habitat_area_parcels, evast_habitat_mapping_raw)
        .toPandas()
    )


class HabitatCreationTypeParcelModel(DataFrameModel):
    """Datamodel for the habitat creation type dataset.

    Attributes:
        id_parcel: The parcel ID.
        action_group: The type of habitat creation action. Either 'Create Wetland',
            'Create SRG', or 'Create Heathland'.
        action_habitat: The specific habitat type assigned to this parcel. This indicates
            what type of habitat is created on the parcel under the habitat creation action.
    """

    id_parcel: str = Field()
    action_group: Category = Field(coerce=True, isin=["Create Heathland", "Create Wetland", "Create SRG"])
    action_habitat: Category = Field(
        coerce=True,
        isin=[
            "lowland",
            "lowland_meadow",
            "upland_meadow",
            "lowland_dry_acid_gr",
            "fen",
            "lowland_calc_gr",
            "lowland_acid_gr",
            "upland_acid_gr",
            "upland_calc_gr",
            "bog",
            "upland",
        ],
    )


fcp_habitat_creation_type_parcel = DerivedDataset(
    name="fcp_habitat_creation_type_parcel",
    level0="silver",
    level1="fcp",
    restricted=False,
    is_geo=False,
    func=_habitat_creation_classification,
    dependencies=[
        is_upland_parcels,
        cec_soilscapes_habitats_parcels,
        evast_habitat_mapping_raw,
        defra_habitat_area_parcels,
    ],
    model=HabitatCreationTypeParcelModel,
)
"""Estimates which type of habitat will be created for each habitat creation action.

This parcel level dataset models which habitat type will be created under each of the ELM/EVAST
habitat creation actions: create wetland, create species-rich grassland, and create heathland.

The dataset is created through a combination of Natmap Soilscapes and Priority Habitats Inventory
geometries joined to parcels. To map between the different habitat names used in these datasets
a lookup table provided by EVAST is used.
"""


def _habitat_management_classification(
    reference_parcels: DerivedDataset,
    defra_priority_habitat_parcels: DerivedDataset,
    evast_habitat_mapping_raw: DerivedDataset,
):
    """Assign a habitat management habitat type to each parcel.

    Assignment is based on the proportion of priority habitat inventory (PHI)
    geometries overlapping the parcel. Where a PHI geometry corresponding to a
    habitat that is covered by a habitat management action, that parcel can be considered
    eligible management actions for that type of habitat. The parcel must be at least 10%
    intersected by a habitat to be considered eligible for management of that habitat.

    The output data includes the area of each habitat type that overlaps the parcel as well as
    the total area for each habitat action group (Create Heathland, Create SRG, Create Wetland).
    """

    sdf = (
        reference_parcels.sdf()
        .join(defra_priority_habitat_parcels.sdf().filter("proportion>=0.1"), on="id_parcel", how="inner")
        .join(
            evast_habitat_mapping_raw.sdf().filter(F.expr("source = 'phi'")).select("action_group", "action_habitat", "habitat_name"),
            on="habitat_name",
            how="inner",
        )
        .selectExpr("id_parcel", "action_group", "action_habitat", "proportion", "area_ha*proportion as habitat_area_ha")
    )
    return sdf.unionByName(
        sdf.groupby("id_parcel", "action_group").agg(
            F.expr("'action_group_total' AS action_habitat"),
            F.expr("FIRST(proportion) AS proportion"),
            F.expr("SUM(habitat_area_ha) AS habitat_area_ha"),
        )
    ).toPandas()


class HabitatManagementTypeParcelModel(DataFrameModel):
    """Datamodel for the habitat creation type dataset.

    Attributes:
        id_parcel: The parcel ID.
        action_group: The type of habitat creation action. Either 'Create Wetland',
            'Create SRG', or 'Create Heathland'.
        action_habitat: The specific habitat type assigned to this parcel. This indicates
            what type of habitat is created on the parcel under the habitat creation action.
        proportion: Proportion of parcel intersected by this habitat.
        habitat_area_ha: Area of parcel intersected by this habitat in hectares.
    """

    id_parcel: str = Field()
    action_group: str = Field(isin=["Create Heathland", "Create Wetland", "Create SRG"])
    action_habitat: str = Field(
        nullable=True,
        isin=[
            "lowland",
            "lowland_meadow",
            "upland_meadow",
            "lowland_dry_acid_gr",
            "fen",
            "lowland_calc_gr",
            "lowland_acid_gr",
            "upland_acid_gr",
            "upland_calc_gr",
            "bog",
            "upland",
            "action_group_total",
        ],
    )
    proportion: float = Field()
    habitat_area_ha: float = Field()


fcp_habitat_management_type_parcel = DerivedDataset(
    name="fcp_habitat_management_type_parcel",
    level0="silver",
    level1="fcp",
    restricted=False,
    is_geo=False,
    func=_habitat_management_classification,
    dependencies=[
        reference_parcels,
        defra_priority_habitat_parcels,
        evast_habitat_mapping_raw,
    ],
    model=HabitatManagementTypeParcelModel,
)
"""Applies the EVAST habitat name lookup dataset to assign an EVAST habitat type
(refered to as factor level by EVAST) to parcels based on whether they intersect priority habitat
inventory (PHI) geometries.

This is used to model which parcels are eligible for habitat management actions.
Comparison to actual agreements show that parcels which do not intersect priority
habitats do also successfully claim for habitat management actions. Due to the low
coverage of PHI data we expect this dataset greatly underestimates the area eligible
for habitat maintenance actions.
"""
