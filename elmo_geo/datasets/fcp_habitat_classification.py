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
from pyspark.sql import Window
from pyspark.sql import functions as F

from elmo_geo.etl import Dataset, DerivedDataset, SourceDataset
from elmo_geo.utils.types import SparkDataFrame

from .cec_soilscapes import cec_soilscapes_habitats_parcels
from .defra_priority_habitats import defra_habitat_area_parcels, defra_priority_habitat_parcels
from .moor import is_upland_parcels
from .rpa_reference_parcels import reference_parcels


# Habitat creation
class EVASTHabitatsMappingModel(DataFrameModel):
    """EVAST's mapping between different habitat classification model.

    This lookup serves two functions. First, to lookup from habitat types in the PHI, LCM, and SoilScapes
    datasets to the high level habitats used when modelling habitat creation actions. Second, to lookup from
    PHI and LCM habitat types to euivalent SoilScape habitats used as part of the methodology to assign habitat
    creation suitability to parcels.

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
       soilscape_habitat_name: Corresponding SoilScapes habitat name. Equivalent to habitat name where source is 'soilscapes'.
       soilscape_habitat_code: Corresponding SoilScapes habitat code. Equivalent to habitat code where source is 'soilscapes'.
    """

    action_group: str = Field(coerce=True, isin=["Create Heathland", "Create Wetland", "Create SRG"])
    action_habitat: str = Field(
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
    bimla_habitat: str = Field(
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
    habitat_name: str = Field()
    habitat_code: str = Field(nullable=True)
    source: str = Field()
    soilscape_habitat_name: str = Field(nullable=True)
    soilscape_habitat_code: str = Field(nullable=True)


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

    sdf_ss = cec_soilscapes_habitats_parcels.sdf().select("id_parcel", "unit", "habitat_code")

    # select lookup to action habitats for soilscapes habitats
    sdf_habitat_lu = (
        evast_habitat_mapping_raw.sdf()
        .filter(F.expr("source = 'soilscapes'"))
        .withColumn("action_group", F.expr("REPLACE(action_group, 'Create ', '')"))
        .selectExpr("action_group", "action_habitat", "is_upland as is_upland_lu", "habitat_code", "soilscape_habitat_code")
    )

    # idenfity candidate habitats that can be created on parcels based on the parcel soil type
    # and whether the parcel is upland or lowland.
    return (
        is_upland_parcels.sdf()
        .join(sdf_ss, on="id_parcel", how="left")
        .join(sdf_habitat_lu, on="habitat_code", how="inner")  # only interested in habitats in the lookup
        .filter(F.expr("(is_upland_lu is NULL) OR (is_upland=is_upland_lu)"))
        .select("id_parcel", "action_group", "action_habitat", "is_upland", "soilscape_habitat_code", "unit")
        .dropDuplicates()
    )


def _clean_habitat_name(col: str) -> callable:
    expr = f"LOWER({col})"
    expr = f"REPLACE({expr}, '&', 'and')"
    expr = f"REPLACE({expr}, ',', '')"
    expr = f"REPLACE({expr}, 'calaminarian grass', 'calaminarian grassland')"
    expr = f"REPLACE({expr}, 'grasslandland', 'grassland')"
    expr = f"TRIM( TRAILING 's' FROM {expr})"
    return F.expr(expr)


def _get_phi_area_with_soilscapes_habitats(
    defra_habitat_area_parcels: Dataset,
    evast_habitat_mapping_raw: Dataset,
    distance_thresholds: list[int] = [1_000, 3_000],
) -> SparkDataFrame:
    """Produce lookup from PHI habitat names to soilscape habitat codes for the PHI area within threshol distances dataset.

    Parameters:
        defra_habitat_area_parcels: PHI area within threshold distances of parcels dataset.
        evast_habitat_mapping_raw: Lookup between habitat names and SoilScape habitat codes and higher level habitat groupings.
        distance_thresholds: Distance thresholds to include.
    """
    sdf_defra_habitat_area_parcels = (
        defra_habitat_area_parcels.sdf()
        .withColumn("habitat_name", _clean_habitat_name("habitat_name"))
        .filter(F.col("distance_threshold").isin(distance_thresholds))
    )

    sdf_evast_habitat_mapping_phi = (
        evast_habitat_mapping_raw.sdf()
        .withColumn("habitat_name", _clean_habitat_name("habitat_name"))
        .filter(F.expr("source = 'phi'"))
        .withColumn("action_group", F.expr("REPLACE(action_group, 'Create ', '')"))
        .select("action_group", "action_habitat", "habitat_name", "source", "soilscape_habitat_code")
        .dropDuplicates()
    )

    # Check habitat names match up between the evast lookup and phi habitat areas joined to parcels
    count_mismatch = (
        sdf_evast_habitat_mapping_phi.select("habitat_name")
        .join(sdf_defra_habitat_area_parcels.select("habitat_name", "area").dropDuplicates(subset=["habitat_name"]), on="habitat_name", how="left")
        .filter("area IS NULL")
    ).count()
    assert count_mismatch == 0, f"{count_mismatch} habtiat names in the EVAST habitat lookup don't match a habitat in the `defra_habitat_area_parcels` dataset."

    # Join evast habitat lookup to phi area to get the soilscape habitat codes and higher level groupings.
    # Flag where parcel has nearby rare habitat.
    rare_habitats = [
        "lowland raised bog",
        "upland fen",
        "purple moor grass and rush pasture",
        "lowland dry acid grassland",
        "upland calcareous grassland",
        "upland hay meadow",
    ]
    return (
        sdf_evast_habitat_mapping_phi.join(sdf_defra_habitat_area_parcels, on="habitat_name", how="inner")
        .withColumn("nearby_rare_habitat", F.col("habitat_name").isin(rare_habitats) & (F.col("distance_threshold") == min(distance_thresholds)))
        .withColumnsRenamed(
            {
                "action_habitat": "action_habitat_phi",
                "soilscape_habitat_code": "soilscape_habitat_code_phi",
            }
        )
    )


def _assign_parcel_habitat_types_from_candidates(
    sdf_candidates: SparkDataFrame, defra_habitat_area_parcels: DerivedDataset, evast_habitat_mapping_raw: DerivedDataset, distance_thresholds=[1_000, 3_000]
) -> SparkDataFrame:
    """Join candidate parcel habitat types to a dataset of nearby priority habitats
    in order to select which action habitat to apply to that parcel.

    Biases assignment of rare habitats where they are found within the minimum threshold distance (1km by default). This
    is because rare habitats will typically have a smaller area and so could be under assigned by an area based assignment.

    The assignments is based on matching PHI habitats to the candidate soil type based habitats as much as possible (using broader 'action_habitat' groupings
    where a direct match does not exist for a parcel) and then ranking habitats by threshold distance, the 'nearby_rare_habitat' flag, and PHI habitat area.
    This approximates a distance weighted area based assignment, with priority for rare habitats that are found within the minimum distance threshold.

    Parameters:
        sdf_candidates: Lookup from parcel ID to candidate habitats to assign to that parcel.
        defra_habitat_area_parcels: Dataset of area of priority habitats withing different threshold
            distances from each parcel.
        evast_habitat_mapping_raw: Lookup between habitat names used in the priority habitats inventory (PHI)
            and EVAST.
        distance_thresholds: List of threshold distances to use in distance weighted prioritisation.
    """

    sdf_phi_lu = _get_phi_area_with_soilscapes_habitats(defra_habitat_area_parcels, evast_habitat_mapping_raw, distance_thresholds)

    # Join phi areas to candidate habitats.
    # Used to choose between candidate habitats based on matching habtiat types and largest habitat area
    sdf_assigned = (
        sdf_candidates.join(sdf_phi_lu, on=["id_parcel", "action_group"], how="inner")
        .withColumn("matches_action_habitat", F.expr("action_habitat = action_habitat_phi"))
        .withColumn("matches_soilscape_habitat", F.expr("soilscape_habitat_code = soilscape_habitat_code_phi"))
        # Only assign habitat where there is a PHI instance that matches a soil type based candidate habitat
        # Or where it's an SRG type habitat (since last resort is default values for these so can be more permissive here)
        .filter("matches_action_habitat OR (action_group='SRG')")
        .filter("matches_soilscape_habitat OR (action_group='SRG')")
    )

    # Create a dataset of deafult SRG habitat types for parcels that are on SRG compatible soils
    # Used in cases where parcels do not have instances of PHI within threshold distances but are on SRG compatible soil
    # Assign these habitats to acid grassland, everything else meadow
    default_acid_gr_habitats = (
        "blanket bog",
        "lowland raised bog",
        "fragmented heath",
        "lowland heathland",
        "mountain heaths and willow scrub",
        "upland heathland",
    )

    window = Window.partitionBy("id_parcel", "action_group").orderBy(F.col("distance_threshold").asc(), F.col("area").desc())
    sdf_defaults = (
        sdf_candidates.filter("action_group='SRG'")
        # join on id_parcel so non-SRG PHI habitats get included
        .join(sdf_phi_lu.select("id_parcel", "habitat_name", "distance_threshold", "area"), on=["id_parcel"], how="left")
        .withColumn(
            "action_habitat",
            F.expr(
                f"""
                CASE 
                WHEN ((habitat_name IN {default_acid_gr_habitats}) AND is_upland) 
                THEN 'upland_acid_gr' 
                WHEN ((habitat_name IN {default_acid_gr_habitats}) AND (NOT is_upland))
                THEN 'lowland_acid_gr'
                WHEN ( ((habitat_name NOT IN {default_acid_gr_habitats}) OR (habitat_name IS NULL))  AND is_upland)
                THEN 'upland_meadow'
                ELSE 'lowland_meadow'
                END"""
            ),
        )
        .withColumn("rank_default", F.row_number().over(window))
        .filter("rank_default=1")
        .selectExpr("id_parcel", "unit", "action_group", "action_habitat", "is_upland", "TRUE as is_default", "habitat_name")
    )

    # Combine the assigned habitats with the default habitats
    # Filter to single action-group, action_habitat per parcel following this priority:
    window = Window.partitionBy("id_parcel", "action_group").orderBy(
        F.col("matches_action_habitat").desc_nulls_last(),  # Soilscape action_group matches a PHI action_group
        F.col("matches_soilscape_habitat").desc_nulls_last(),  # Soilscape habitat matches a PHI habitat
        F.col("distance_threshold").asc(),  # Nearby observation of a PHI habitat
        F.col("nearby_rare_habitat").desc(),  # Rarer PHI habitats
        F.col("area").desc(),  # PHI habitats that are more abundant
        F.col("is_default").desc_nulls_last(),  # Is the default habitat (for SRG only)
    )

    sdf = sdf_assigned.unionByName(sdf_defaults, allowMissingColumns=True).withColumn("rank", F.row_number().over(window)).filter("rank=1")

    # Checks
    msg = """Habitat assignment should only occur where PHI habitat group and corresponding soilscape habitat match the candidate habitat
            group and soilscape habitat type OR where a default habitat is assigned."""
    assert (
        sdf.filter(
            """((NOT matches_action_habitat) OR (matches_action_habitat IS NULL)) AND
               ((NOT matches_soilscape_habitat) OR (matches_soilscape_habitat IS NULL)) AND 
               (NOT is_default)"""
        ).count()
        == 0
    ), msg
    assert sdf.filter("(id_parcel is NULL) OR (action_group is NULL) OR (action_habitat is NULL)").count() == 0, "Unexpected nulls in assignment"
    assert sdf.filter("(is_upland) AND (action_habitat like '%lowland%')").count() == 0, "Lowland habitat cannot be assigned to upland parcel"
    assert sdf.filter("(NOT is_upland) AND (action_habitat like '%upland%')").count() == 0, "Upland habitat cannot be assigned to lowland parcel"
    assert sdf.filter("(is_default) AND (action_group != 'SRG')").count() == 0, "Unexpected default habitat assignment"
    assert sdf.filter("is_default").count() > 0, "Zero default assignment habitats"
    assert sdf.filter("(action_habitat_phi IS NULL) AND (NOT is_default)").count() == 0, "Unexpected null phi habitat"

    return sdf.select("id_parcel", "unit", "action_group", "action_habitat")


def _habitat_creation_classification(
    is_upland_parcels: DerivedDataset,
    cec_soilscapes_habitats_parcels: DerivedDataset,
    evast_habitat_mapping_raw: DerivedDataset,
    defra_habitat_area_parcels: DerivedDataset,
) -> SparkDataFrame:
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
        .transform(_assign_parcel_habitat_types_from_candidates, defra_habitat_area_parcels, evast_habitat_mapping_raw)
        .toPandas()
    )


class HabitatCreationTypeParcelModel(DataFrameModel):
    """Datamodel for the habitat creation type dataset.

    Attributes:
        id_parcel: The parcel ID.
        unit: SoilScapes soil units the habitat assignment is based on.
        action_group: The type of habitat creation action. Either 'Wetland',
            'SRG', or 'Heathland'.
        action_habitat: The specific habitat type assigned to this parcel. This indicates
            what type of habitat is created on the parcel under the habitat creation action.
    """

    id_parcel: str = Field()
    unit: float = Field(isin=set(range(1, 32)).difference([29]))
    action_group: str = Field(coerce=True, isin=["Heathland", "Wetland", "SRG"])
    action_habitat: str = Field(
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


# Habitat management
class EVASTHabitatsManagementMappingModel(DataFrameModel):
    """EVAST's mapping between PHI habitats and their habitat categories for management actions
    data model.

    Parameters:
       action_group: The type of habitat. Is either "Wetland" or "SRG". SRG stands for species-rich grassland.
       action_habitat: The habitat name used by EVAST in habitat management actions.
       habitat_name: The Priority Habitats Inventory habitat name.
    """

    action_group: str = Field(isin=["Wetland", "SRG"])
    action_habitat: str = Field(
        isin=[
            "Wet Acid Heath and Grassland",
            "Wet Acid Heath and Grassland",
            "Bogs",
            "Wet Calcareous Fen",
            "Wet Neutral Grassland",
            "Wet Neutral Fen",
            "Neutral Grassland",
            "Damp Calcareous Grassland",
            "Dry Calcareous Grassland",
            "Dry Acid Heath and Grassland",
        ],
    )
    habitat_name: str = Field()


evast_habitat_management_mapping_raw = SourceDataset(
    name="evast_habitat_management_mapping_raw",
    level0="bronze",
    level1="evast",
    restricted=False,
    is_geo=False,
    model=EVASTHabitatsManagementMappingModel,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/evast/EVAST_HabitatStocking_2024_08_29_M3_habitat_manage_classification.csv",
)
"""EVAST's mapping from PHI habitats to the categories they use for habitat management actions.

This dataset was provided in the 'EVAST M3'
tab of EVASTs [Habitat Stocking]:https://defra.sharepoint.com/:x:/r/teams/Team1645/_layouts/15/Doc.aspx?sourcedoc=%7B305701D9-E8E2-424B-8B3F-837B876133B6%7D&file=EVAST_HabitatStocking-2024_08_29.xlsx
workbook but has been reformatted to one row per PHI habitat format to align with the analysis pipeline used here.
"""


def _is_phi(
    reference_parcels: DerivedDataset,
    defra_priority_habitat_parcels: DerivedDataset,
    evast_habitat_mapping_raw: DerivedDataset,
    evast_habitat_management_mapping_raw: DerivedDataset,
):
    """Calculates total proportion of different groups of Priority Habitat Inventory habitats intersecting parcels.

    These proportions are used to identify which parcels already have habitats on them that may be managed
    under habitat management actions.

    There are different groupings/names of habitats that FCP and EVAST use:
    - raw: these are the habitat names as given in the PHI
    - evast_create: these are the habitat used for habitat creation actions by EVAST
    - evast_manage: these are the habitat used for habitat management actions by EVAST
    """

    sdf_defra_priority_habitat_parcels = defra_priority_habitat_parcels.sdf().withColumn("habitat_name_clean", _clean_habitat_name("habitat_name"))

    sdf_evast_habitat_mapping = (
        evast_habitat_mapping_raw.sdf()
        .withColumn("habitat_name_clean", _clean_habitat_name("habitat_name"))
        .withColumn("action_group", F.expr("REPLACE(action_group, 'Create ', '')"))
        .filter(F.expr("source = 'phi'"))
        .select("action_group", "action_habitat", "habitat_name_clean")
    )
    sdf_evast_habitat_mapping = sdf_evast_habitat_mapping.unionByName(
        sdf_evast_habitat_mapping.selectExpr("action_group", "'action_group_total' as action_habitat", "habitat_name_clean")
    )

    sdf_raw = (
        reference_parcels.sdf()
        .join(sdf_defra_priority_habitat_parcels, on="id_parcel", how="inner")
        .withColumnRenamed("habitat_name", "action_habitat")
        .withColumn("grouping_category", F.lit("raw"))
    )

    sdf_create = (
        reference_parcels.sdf()
        .join(sdf_defra_priority_habitat_parcels, on="id_parcel", how="inner")
        .join(
            sdf_evast_habitat_mapping,
            on="habitat_name_clean",
            how="inner",
        )
        .withColumn("grouping_category", F.lit("evast_create"))
    )

    # setup habitat management categories with additional aggregations
    sdf_manage_lookup = (
        evast_habitat_management_mapping_raw.sdf()
        .unionByName(
            evast_habitat_management_mapping_raw.sdf().selectExpr(
                "action_group",
                "'action_group_total' as action_habitat",
                "habitat_name",
            ),
            allowMissingColumns=False,
        )
        .unionByName(
            evast_habitat_management_mapping_raw.sdf()
            .filter("action_group in ('Wetland', 'SRG')")
            .selectExpr(
                "'Wetland and SRG' as action_group",
                "'action_group_total' as action_habitat",
                "habitat_name",
            ),
            allowMissingColumns=False,
        )
    )

    sdf_manage = (
        reference_parcels.sdf()
        .join(sdf_defra_priority_habitat_parcels, on="id_parcel", how="inner")
        .join(
            sdf_manage_lookup.select("action_group", "action_habitat", "habitat_name"),
            on="habitat_name",
            how="inner",
        )
        .withColumn("grouping_category", F.lit("evast_manage"))
    )

    return (
        sdf_raw.unionByName(sdf_create, allowMissingColumns=True)
        .unionByName(sdf_manage, allowMissingColumns=True)
        .dropDuplicates(subset=["id_parcel", "grouping_category", "action_group", "action_habitat", "fid"])
        .groupby("id_parcel", "grouping_category", "action_group", "action_habitat")
        .agg(
            F.expr("FIRST(area_ha) as area_ha"),
            F.expr("SUM(proportion) as proportion"),
        )
        .selectExpr("id_parcel", "grouping_category", "action_group", "action_habitat", "proportion", "area_ha*proportion as habitat_area_ha")
        .filter("proportion>0")
        .toPandas()
    )


class IsPHIParcelModel(DataFrameModel):
    """Datamodel for the parcel level PHI aggregations.

    Attributes:
        id_parcel: The parcel ID.
        grouping_category: Used to select which group of habitat names to view proportions for.
        action_group: The type of habitat creation action. Either 'Wetland', 'SRG', or 'Heathland'.
        action_habitat: The specific habitat type assigned to this parcel. This indicates
            what type of habitat is created on the parcel under the habitat creation action.
        proportion: Proportion of parcel intersected by this habitat.
        habitat_area_ha: Area of parcel intersected by this habitat in hectares.
    """

    id_parcel: str = Field()
    grouping_category: str = Field(isin=["raw", "evast_create", "evast_manage"])
    action_group: str = Field(isin=["Heathland", "Wetland", "SRG", "Wetland and SRG"], nullable=True)
    action_habitat: str = Field()
    proportion: float = Field()
    habitat_area_ha: float = Field()


fcp_is_phi_parcel = DerivedDataset(
    name="fcp_is_phi_parcel",
    level0="gold",
    level1="fcp",
    restricted=False,
    is_geo=False,
    func=_is_phi,
    dependencies=[
        reference_parcels,
        defra_priority_habitat_parcels,
        evast_habitat_mapping_raw,
        evast_habitat_management_mapping_raw,
    ],
    model=IsPHIParcelModel,
)
"""Applies EVAST habitat name lookup datasets to assign an EVAST habitat type to parcels
based on whether they intersect priority habitat inventory (PHI) geometries.

Also provides total proportions of raw PHI habtiat types in each parcel. Proportions should not
be summed or subracted across habitat types since the geometries these are based on may overlap.

This is used to model where priority habitats already exist and therefore where parcels are currently
eligible for habitat management actions.

Comparison to actual agreements show that parcels which do not intersect priority
habitats do also successfully claim for habitat management actions. Due to the low
coverage of PHI data we expect this dataset greatly underestimates the area eligible
for habitat maintenance actions.
"""
