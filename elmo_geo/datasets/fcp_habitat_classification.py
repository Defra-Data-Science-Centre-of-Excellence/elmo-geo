"""Parcel level habitat creation and management classifications.

Habitat creation ELM actions require specifying the most suitable habitats to
create on a parcel. Similarly, habitat management actions require specifying which prioirty habitats already exist on each
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


Currently the assignment process uses Soilscapes (with addition habitat lookup) and Priority Habitat Inventory Datasets.
The habitat categories in these datasets do not map to all of the required habitat types listed above. The 'upland_acid_gr'
and 'lowland_acid_gr' species-rich grassland habitat types are not represented in these datasets.

To resolved this issues, the CEH Land Cover Map dataset needs to be integrated into the assignment process.
# TODO: https://github.com/Defra-Data-Science-Centre-of-Excellence/elm_modelling_strategy/issues/887
"""


import pandas as pd
from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pyspark.sql import functions as F

from elmo_geo.etl import DerivedDataset, SourceDataset
from elmo_geo.utils.types import PandasDataFrame, SparkDataFrame

from .cec_soilscapes import cec_soilscapes_habitats_parcels
from .defra_priority_habitats import defra_habitat_area_parcels
from .moor import moorline_parcels
from .rpa_reference_parcels import reference_parcels


class EVASTHabitatsMappingModel(DataFrameModel):
    """EVAST's mapping between different habitat classification model.

    Parameters:
       action_group: The type of habitat creation action the habitats correspond to.
       action_habitat: The habitat type created by the habitat creation action. Referred to as the 'factor_level' by EVAST.
       BIMLA_model_grouping: Habitat type used by EVASTs BIMLA model.
       is_upland: Boolean indicating whether the habitat type is above the moorland line (True)
              or not (False) or can be both (None).
       habitat_name: Corresponding habitat name in alternative source.
       habitat_code_soilscapes: Corresponding habitat code in alternative source (only soilscape habitats have a code).
       source: The alternative habtiat dataset that is being mapped to the create habitat action habitat types. Either
            soilscapes, phi (priority habitat inventory), lcm (CEH land cover map), or aes (from agri-environment scheme
            uptake data).
    """

    action_group: Category = Field(coerce=True, isin=["Create Heathland", "Create Wetland", "Create SRG"])
    action_habitat: str = Field()
    is_upland: bool = Field(coerce=True, nullable=True)
    BIMLA_model_grouping: str = Field()
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
tab of EVASTs [Habitat Stocking](
    https://defra.sharepoint.com/:x:/r/teams/Team1645/_layouts/15/Doc.aspx?sourcedoc=%7B305701D9-E8E2-424B-8B3F-837B876133B6%7D&file=EVAST_HabitatStocking-2024_08_29.xlsx
    ) workbook but has been reformatted to a 'long' format to align with the analysis pipeline used here.
"""


def _get_parcel_candidate_habitates(
    reference_parcels: DerivedDataset,
    moorline_parcels: DerivedDataset,
    cec_soilscapes_habitats_parcels: DerivedDataset,
    evast_habitat_mapping_raw: DerivedDataset,
) -> SparkDataFrame:
    """Identify candidate habitats that could be created on each parcel.

    Use soilscapes soil type and moorland line (upland/lowland classification)
    to get candidate habitat types for each parcel.
    """
    threshold = 0.1

    sdf_isupland = (
        reference_parcels.sdf()
        .select("id_parcel")
        .join(moorline_parcels.sdf().filter(F.expr(f"proportion>{threshold}")), on="id_parcel", how="left")
        .selectExpr("id_parcel", "COALESCE(name in ('MD', 'MS'), FALSE) as is_upland")
    )

    sdf_ss = (
        cec_soilscapes_habitats_parcels.sdf()
        .filter(F.expr(f"proportion>{threshold}"))
        .withColumnRenamed("proportion", "proportion_soilscapes")
        .select("id_parcel", "unit", "habitat_code", "habitat_type")
    )

    # select lookup to action habitats for soilscapes habitats
    sdf_habitat_mapping = (
        evast_habitat_mapping_raw.sdf()
        .filter(F.expr("source = 'soilscapes'"))
        .selectExpr("action_group", "action_habitat", "is_upland as is_upland_map", "habitat_code")
    )

    # idenfity candidate habitats that can be created on parcels based on the parcel soil type
    # and whether the parcel is upland or lowland.
    return (
        sdf_isupland.join(sdf_ss, on="id_parcel", how="outer")
        .join(sdf_habitat_mapping, on="habitat_code", how="left")
        .filter(F.expr("(is_upland_map is NULL) OR (is_upland=is_upland_map)"))
        .select("id_parcel", "action_group", "action_habitat")
        .dropDuplicates()
    )


def _classify_parcel_habitat_by_phi_area(df: PandasDataFrame) -> PandasDataFrame:
    """Identifies a habitat creation type for a parcel based on PHI area.

    Takes an input dataframe with a single parcel id and multiple habitat
    types and returns a dataframe with a single habitat type per habitat group
    (wetland, species-rich grassland, heathland).

    Filters by area of priority habitat inventory habitats within either a 2km or 5km
    distance. The 2km distance is used for rarer habitats, to avoid more common
    habitats only being selected.

    Parameters:
        df: Dataframe of candidate habitat types and nearby phi habitats for a single parcel.
    """
    df_out = pd.DataFrame(columns=["action_group_phi", "action_habitat_phi"], data=[])

    # first check for rare habitats within 2km
    rare_habitats = [
        "Lowland Raised Bog",
        "Upland Fen",
        "Purple Moor Grass & Rush Pasture",
        "Lowland dry acid grassland",
        "Upland calcareous grassland",
        "Upland hay meadows",
    ]
    df_rare = df.loc[df["Main_Habit"].isin(rare_habitats) & (df["distance"] == 2_000)]
    if df_rare.shape[0] > 0:
        df_rare = df_rare.groupby("action_group_phi").apply(lambda df: df.sort_values(by="area", ascending=False).iloc[:1])
        df_out = pd.concat([df_out, df_rare]).reset_index()

    # then check for all habitats within 5km
    df_all = df.loc[df["distance"] == 5_000]
    if df_all.shape[0] > 0:
        df_all = df_all.groupby("action_group_phi").apply(lambda df: df.sort_values(by="area", ascending=False).iloc[:1])
        df_out = pd.concat([df_out, df_all]).reset_index()

    # Combine, keeping the rare habitat classifications as 1st priority
    return df_out[["id_parcel", "action_group_phi", "action_habitat_phi"]].drop_duplicates(subset="action_group_phi", keep="first")


def _filter_candidates_by_phi(
    sdf_refine: SparkDataFrame,
    defra_habitat_area_parcels: DerivedDataset,
    evast_habitat_mapping_raw: DerivedDataset,
) -> SparkDataFrame:
    # Get habitat mapping for phi habitat names
    sdf_habitat_mapping = (
        evast_habitat_mapping_raw.sdf()
        .filter(F.expr("source = 'phi'"))
        .selectExpr("action_group as action_group_phi", "action_habitat as action_habitat_phi", "habitat_name as Main_Habit")
    )

    # join in nearby phi habitats and filter to where the phi action habitat type
    # matches a soilscape based action habitat
    sdf_refine = (
        sdf_refine.join(defra_habitat_area_parcels.sdf(), on="id_parcel", how="outer")
        .join(sdf_habitat_mapping, on="Main_Habit", how="left")
        .filter(F.expr("action_habitat = action_habitat_phi"))
    )

    # Now select which habitat to assign to each parcel based on the one with the most
    # area in either a 2km radius (for rare habitats) or 5km (for all habitiats)
    return (
        sdf_refine.repartition(200, "id_parcel")
        .groupby("id_parcel")
        .applyInPandas(_classify_parcel_habitat_by_phi_area, schema="id_parcel string, action_group_phi string, action_habitat_phi string")
    )


def _transform(
    reference_parcels: DerivedDataset,
    moorline_parcels: DerivedDataset,
    cec_soilscapes_habitats_parcels: DerivedDataset,
    evast_habitat_mapping_raw: DerivedDataset,
    defra_habitat_area_parcels: DerivedDataset,
):
    """Assign a habitat creation habitat type to each parcel.

    For each habitat creation action, create wetalnd, create species-rich grassland, create heathland,
    this function assigns parcels with a habitat type that will be created.

    The assignment works as follows:

    1. Based on the Natmap Scoilscape soiltype of the parcel identify which habitat can potential occur on the parcel.
    These are termed the 'candidate' habitats.
    2. Filter this list to only include candidate habitats which correspond to the upland/lowland classification of the parcel.
    3. Further filter the candidates to only assign habitats where an instance of the habitat exists within a threshold distance
    of the parcel according to the Priority Habitat Inventory (PHI). Where multiple such candidates exist assign the habitat with the
    largest area in the PHI

    The threshold distance used depends on the habitat type. The function first tries to assign rarer habitat types using a 2km distance
    threshold and then all habitat types using a 5km distance threshold. This is because rarer habitats will have lower area coverage, so
    may consistently not be assigned at the 5km threshold distance.
    """
    sdf_candidates = _get_parcel_candidate_habitates(
        reference_parcels,
        moorline_parcels,
        cec_soilscapes_habitats_parcels,
        evast_habitat_mapping_raw,
    )

    # Add in a count of the number of habitats per percel
    sdf_counts = sdf_candidates.groupby(["id_parcel", "action_group"]).agg(F.expr("COUNT(DISTINCT(action_habitat)) as action_habitat_count"))
    sdf_candidates = sdf_candidates.join(sdf_counts, on=["id_parcel", "action_group"], how="left")

    # Now select parcels which are matched to a single action habitat within an action_group
    # these do not need further processing
    sdf_done1 = sdf_candidates.filter(F.expr("action_habitat_count = 1")).select("id_parcel", "action_group", "action_habitat")

    # The remaining parcels will be joined to a datasets of nearby priority habitats in order to select which
    # action habitat to apply to that parcel
    sdf_refine = sdf_candidates.filter(F.expr("action_habitat_count != 1"))
    sdf_done2 = _filter_candidates_by_phi(
        sdf_refine,
        defra_habitat_area_parcels,
        evast_habitat_mapping_raw,
    )

    return sdf_done1.unionByName(sdf_done2.withColumnsRenamed({"action_group_phi": "action_group", "action_habitat_phi": "action_habitat"}))


class HabitatCreationTypeParcelModel(DataFrameModel):
    """Datamodel for the habitat creation type dataset.

    Attributes:
        id_parcel: The aprcel ID.
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
    func=_transform,
    dependencies=[
        reference_parcels,
        moorline_parcels,
        cec_soilscapes_habitats_parcels,
        evast_habitat_mapping_raw,
        defra_habitat_area_parcels,
    ],
    model=HabitatCreationTypeParcelModel,
)
"""Estimates which type of habitat will be created for each habitat creation action.

This parcel level dataset models which habitat type will be created under each of the ELM/EVAST
habitat creation actions: create wetland, create species-rich grassland, and creat heathland.

The dataset is created through a combination of Natmap Soilscapes and Priority Habitats Inventory
geometries joined to parcels. To map between the different habitat names used in these datasets
a lookup table provided by evast is used.
"""
