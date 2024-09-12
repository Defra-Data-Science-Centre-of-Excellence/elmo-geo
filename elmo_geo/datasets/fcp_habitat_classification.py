"""Parcel level habitat creation and management classifications.

Habitat creation ELM actions require specifying which habitats would most suitable be created
on a parcel. This is done by combining multiple data sources:
- Soilscapes linked to which habitats can be created on each soil type
- Proximity to priority habitats
- Habitat map habitat type

Joining these three datasets together. 
"""

from functools import partial

import pandas as pd
from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels
from elmo_geo.uitls.types import SparkDataFrame, PandasDataFrame

from .cec_soilscapes import cec_soilscapes_habitats_parcels
from .defra_priority_habitats import defra_habitat_area_parcels
from .living_england import living_england_habitat_map_phase_4_parcels
from .moor import moorline_parcels


class EVASTHabitatsMappingModel(DataFrameModel):
    """EVAST's mapping between different habitat classification model.

    Parameters:
       action_group: The type of habitat creation action the habitats correspond to.
       create_habitat_factor_level: Habitat creation action sub-type.
       BIMLA_model_grouping: Habitat creation sub-type used by EVASTs BIMLA model.
       is_upland: Boolean indicating whether the habitat factor level is above the moorland line (True)
              or not (False) or can be both (None).
       habitat_name_soilscapes: Habitat type from soilscapes habitat mapping.
       habitat_code_soilscapes: Habitat code from soilscapes habitat mapping.
       habitat_name_phi: Priority habitats inventory habitats that map to this habitat creation action.
       habitat_name_lcm: CEH Land cover map habitats that map to this habitat creation action.
    """
    action_group:Category=Field(coerce=True,
                                isin=["Create Heathland", "Create Wetland", "Create SRG"])
    create_habitat_factor_level:str=Field()
    is_upland:bool=Field(coerce=True, nullable=True)
    BIMLA_model_grouping:str=Field()
    habitat_name_soilscapes:str=Field(nullable=True)
    habitat_code_soilscapes:str=Field(nullable=True)
    habitat_name_phi:str=Field(nullable=True)
    habitat_name_lcm:str=Field(nullable=True)

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
tab of EVASTs [Habitat Stocking](https://defra.sharepoint.com/:x:/r/teams/Team1645/_layouts/15/Doc.aspx?sourcedoc=%7B305701D9-E8E2-424B-8B3F-837B876133B6%7D&file=EVAST_HabitatStocking-2024_08_29.xlsx) workbook
but has been reformatted to align with the analysis pipeline used here.
"""


# Identify candidate habitat creation types, based on soil type

def _get_parcel_candidate_habitates(reference_parcels: DerivedDataset,
                                    moorline_parcels:DerivedDataset,
                                    cec_soilscapes_habitats_parcels:DerivedDataset,
                                    evast_habitat_mapping_raw:DerivedDataset,
                                    ) -> SparkDataFrame:
      """Identify candidate habitats that could be created on each parcel.
      
      Use soilscapes soil type and moorland line (upland/lowland classification)
      to get candidate habitat types for each parcel. 
      """
      threshold = 0.1

      sdf_isupland = (reference_parcels.sdf().select("id_parcel")
                  .join(moorline_parcels.sdf().filter(F.expr(f"proportion>{threshold}")),
                        on="id_parcel",
                        how="left")
                  .selectExpr("id_parcel", "COALESCE(name in ('MD', 'MS'), FALSE) as is_upland"))

      sdf_ss = (cec_soilscapes_habitats_parcels.sdf()
            .filter(F.expr(f"proportion>{threshold}"))
            .withColumnRenamed("proportion", "proportion_soilscapes")
            .select("id_parcel", "unit", "habitat_code", "habitat_type"))

      # alias fields
      sdf_habitat_mapping  = (evast_habitat_mapping_raw.sdf()
                              .selectExpr("action_group",
                                          "create_habitat_factor_level as action_habitat",
                                          "is_upland as is_upland_map",
                                          "habitat_code_soilscapes as habitat_code"))

      sdf_candidates = (sdf_isupland
                        .join(sdf_ss, on="id_parcel", how="outer")
                        .join(sdf_habitat_mapping, on="habitat_code", how="left"))

      # filter to only action habitats which match the upland classification of the parcel
      # or where the habitat can be either upland or lowland
      sdf_candidates = (sdf_candidates
                        .filter(F.expr("(is_upland_map is NULL) OR (is_upland=is_upland_map)"))
                        .select("id_parcel", "action_group", "action_habitat")
                        .dropDuplicates())
      
      return sdf_candidates
  
def _classify_parcel_habitat_by_phi_area(df:PandasDataFrame)->PandasDataFrame:
    """Identifies a habitat creation types for a parcel based on PHI area.

    Takes an input dataframe with a single parcel id and with multiple habitat
    types and returns a dataframe with a single habitat type per habitat group
    (wetland, species-rich grassland, heathland).
    
    Filters by area of priority habitat inventory geometry within either a 2km or 5km
    distance. The 2km distance is used for rarer habitats, to avoid more common
    habitats only being selected.
    """
    df_out = pd.DataFrame(columns = ["action_group_phi", "action_habitat_phi"], data = [])

    # first check for rare habitats within 2km
    rare_habitats = [
        "Lowland Raised Bog",
        "Upland Fen",
        "Purple Moor Grass & Rush Pasture",
        "Lowland dry acid grassland",
        "Upland calcareous grassland",
        "Upland hay meadows",
    ]
    df_rare = df.loc[df["Main_Habit"].isin(rare_habitats) & (df["distance"]==2_000)]
    if df_rare.shape[0]>0:
        df_rare = df_rare.groupby("action_group_phi").apply(
            lambda df: df.sort_values(by="area", ascending=False).iloc[:1])
        df_out = pd.concat([df_out, df_rare]).reset_index()

    
    # then check for all habitats within 5km
    df_all = df.loc[df["distance"]==5_000]
    if df_all.shape[0]>0:
        df_all = df_all.groupby("action_group_phi").apply(
            lambda df: df.sort_values(by="area", ascending=False).iloc[:1])
        df_out = pd.concat([df_out, df_all]).reset_index()

    # Combine, keeping the rare habitat classifications as 1st priority
    return (df_out[["id_parcel", "action_group_phi", "action_habitat_phi"]].drop_duplicates(subset = "action_group_phi", keep="first"))

def _filter_candidates_by_phi(sdf_refine:SparkDataFrame,
                              defra_habitat_area_parcels:DerivedDataset,
                              evast_habitat_mapping_raw:DerivedDataset,
                              ) -> SparkDataFrame:
    # Get habitat mapping for phi habitat names
    sdf_habitat_mapping  = (evast_habitat_mapping_raw.sdf()
                            .selectExpr("action_group as action_group_phi",
                                        "create_habitat_factor_level as action_habitat_phi",
                                        "habitat_name_phi as Main_Habit"))

    # join in nearby phi habitats and filter to where the phi action habitat type
    # matches a soilscape based action habitat
    sdf_refine = (sdf_refine
                .join(defra_habitat_area_parcels.sdf(), on="id_parcel", how = "outer")
                .join(sdf_habitat_mapping, on = "Main_Habit", how="left")
                .filter(F.expr("action_habitat = action_habitat_phi")))
    
    # Now select which habitat to assign to each parcel based on the one with the most
    # area ibn either a 2km radius (for rare habitats) or 5km (for all habitiats)
    return (sdf_refine.groupby("id_parcel")
            .applyInPandas(_classify_parcel_habitat_by_phi_area, 
                           schema = "id_parcel string, action_group_phi string, action_habitat_phi string"))

def _transform(reference_parces:DerivedDataset,
               moorline_parcels:DerivedDataset,
               cec_soilscapes_habitats_parcels:DerivedDataset,
               evast_habitat_mapping_raw:DerivedDataset,
               defra_habitat_area_parcels:DerivedDataset
               ):
    """
    """
    sdf_candidates = _get_parcel_candidate_habitates(reference_parcels,
                                    moorline_parcels,
                                    cec_soilscapes_habitats_parcels,
                                    evast_habitat_mapping_raw,
                                    )

    # Add in a count of the number of habitats per percel
    sdf_counts = (sdf_candidates
                    .groupby(["id_parcel", "action_group"])
                    .agg(F.expr("COUNT(DISTINCT(action_habitat)) as action_habitat_count")))
    sdf_candidates = sdf_candidates.join(sdf_counts, on=["id_parcel", "action_group"], how="left")

    # Now select parcels which are matched to a single action habitat within an action_group
    # these do not need further processing
    sdf_done1 = (sdf_candidates.filter(F.expr("action_habitat_count = 1"))
                .select("id_parcel", "action_group", "action_habitat"))

    # The remaining parcels will be joined to a datasets of nearby priority habitats in order to select which
    # action habitat to apply to that parcel
    sdf_refine = sdf_candidates.filter(F.expr("action_habitat_count != 1"))

    sdf_done2 = _filter_candidates_by_phi(sdf_refine,
                                        defra_habitat_area_parcels,
                                        evast_habitat_mapping_raw,
                                        )
        

    return sdf_done1.unionByName(sdf_done2.withColumnsRenamed({"action_group_phi":"action_group", "action_habitat_phi": "action_habitat"}))


