"""Cranfield Environment Centre (CEC) SoilScapes data.

The CEC SoilScapes dataset[^1] is a product derived from the National Soil Map. It seeks to provide a useful, concise, easily interpreted and
applicable description of the soils of England and Wales. It is a simplified rendition of the national soil map and contains about
30 distinct soil types.

[^1] https://www.landis.org.uk/data/nmsoilscapes.cfm
"""

from functools import partial

import pandas as pd
from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry
from pyspark.sql import functions as F

from elmo_geo.etl import DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import sjoin_parcel_proportion

from .rpa_reference_parcels import reference_parcels

_join_parcels = partial(
    sjoin_parcel_proportion,
    columns=["unit", "natural_dr", "natural_fe", "surf_text"],
    fn_pre=lambda sdf: sdf.withColumn("geometry", F.expr("EXPLODE(ST_Dump(geometry))")),
)


class CECSoilScapesRaw(DataFrameModel):
    """Cranfield Environment Centre (CEC) SoilScapes data model.

    Dataset owner to be confirmed as part of this task:
    https://github.com/Defra-Data-Science-Centre-of-Excellence/elmo-geo/issues/223

    Parameters:
        geometry: Geometry indicating the extent of the soil type.
    """

    geometry: Geometry = Field()


cec_soilscapes_raw = SourceDataset(
    name="cec_soilscapes_raw",
    medallion="bronze",
    source="cec",
    model=CECSoilScapesRaw,
    restricted=True,
    is_geo=True,
    source_path="/dbfs/mnt/lab/restricted/natmap/natmap_soilscapes.parquet",
)
"""Cranfield Environment Centre (CEC) SoilScapes data.
"""


class CECSoilScapesParcels(DataFrameModel):
    """Cranfield Environment Centre (CEC) SoilScapes data joined to parcels model.

    Parameters:
        id_parcel: The parcel ID.
        unit: Soil type category, expressed as an integer.
        natural_dr: General description of the drainage of the soil.
        natural_fe: General description of the fertility of the soil.
        surf_text: General description of the surface texture of the soil.
        proportion: Proportion of the parcel intersected by the soil type.
    """

    # TODO: Alias the field names once PR #209. Task for aliasing:
    # https://github.com/Defra-Data-Science-Centre-of-Excellence/elmo-geo/issues/223
    id_parcel: str = Field()
    unit: float = Field(isin=set(range(1, 32)).difference([29]))
    natural_dr: str = Field(
        isin=[
            "Freely draining",
            "Naturally wet",
            " ",
            "Impeded drainage",
            "Variable",
            "Slightly impeded drainage",
            "Surface wetness",
        ],
    )
    natural_fe: str = Field(
        isin=[
            "Very low",
            "Low",
            "Lime-rich",
            " ",
            "High",
            "Moderate",
            "Moderate to high",
            "Lime-rich but saline",
            "Mixed, low to lime-rich",
            "Lime-rich to moderate",
            "Low to high",
            "Low to moderate",
            "Mixed, lime-rich to low",
        ],
    )
    surf_text: str = Field(
        isin=[
            "Loamy",
            "Sandy",
            "Peaty",
            " ",
            "Clayey",
        ],
    )
    proportion: float = Field(ge=0, le=1)


cec_soilscapes_parcels = DerivedDataset(
    name="cec_soilscapes_parcels",
    medallion="silver",
    source="cec",
    restricted=True,
    is_geo=False,
    model=CECSoilScapesParcels,
    dependencies=[reference_parcels, cec_soilscapes_raw],
    func=_join_parcels,
)
"""Cranfield Environment Centre (CEC) SoilScapes data joined to RPA parcels.
"""

ne_soilscapes_habitats_raw = SourceDataset(
    name="ne_soilscapes_habitats_raw",
    medallion="bronze",
    source="ne",
    restricted=False,
    is_geo=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/evast/habitat_stocking/EVAST_HabitatStocking_2024_08_29_SoilscapesIDlookup.csv",
)
"""Natural England mapping from soilscape unit to viable habitat types.

This dataset was provided to EVAST (CEH) by Natural England and appeared as the
'SoilscapesIDlookup' tab of EVAST's HabitatStocking-2024_08_29.xlsx workbook.

The mapping indicates which habitats can be created on different soil types.
It is a coarse approximation, in part due to the use of coarse soil type
categories.
"""


def _join_habitat_types(
    soilscapes_parcels: DerivedDataset,
) -> pd.DataFrame:
    df_map = ne_soilscapes_habitats_raw.pdf()
    habitat_abbreviation_lookup = dict(zip([c.strip() for c in df_map.columns], df_map.iloc[0]))
    df_map = (
        df_map.iloc[1:, 1:]
        .rename(columns={"Unnamed: 1": "unit"})
        .set_index("unit")
        .stack()
        .reset_index()
        .drop(0, axis=1)
        .rename(columns={"level_1": "habitat_name"})
        .assign(habitat_name=lambda df: df.habitat_name.str.strip())
        .assign(habitat_code=lambda df: df.habitat_name.replace(habitat_abbreviation_lookup))
    )
    return soilscapes_parcels.pdf().merge(df_map, on="unit", how="left", validate="m:m")


class CECSoilScapesHabitatsParcels(DataFrameModel):
    """Cranfield Environment Centre (CEC) SoilScapes joined to parcels
    and habitat types data model.

    Parameters:
        id_parcel: The parcel ID.
        unit: Soil type category, expressed as an integer.
        habitat_code: Abbreviation indicating the habitat type that can exist on the soil type.
        habitat_name: Name of the habitat type that can exist on the soil type.
    """

    id_parcel: str = Field()
    unit: float = Field(nullable=True, isin=set(range(1, 32)).difference([29]))
    habitat_code: str = Field(
        nullable=True, isin=["UHL", "LHL", "LCG", "LAG", "LMW", "UHM", "UCG", "LRB", "UFS", "BBG", "LFN", "PMG", "WPP", "CVS", "RBD", "CSM", "TRO", "CSD"]
    )
    habitat_name: str = Field(
        nullable=True,
        isin=[
            "Upland heathland",
            "Lowland heathland",
            "Lowland calcareous grassland",
            "Lowland dry acid grassland",
            "Lowland meadows",
            "Upland hay meadows",
            "Upland calcareous grassland",
            "Lowland raised bog",
            "Upland fens",
            "Blanket bog",
            "Lowland fens",
            "Purple moorgrass and rush pasture",
            "Wood-pasture & parkland",
            "Coastal vegetated shingle",
            "Reedbeds",
            "Saltmarsh",
            "Traditional orchards",
            "Coastal sand dunes",
        ],
    )


cec_soilscapes_habitats_parcels = DerivedDataset(
    name="cec_soilscapes_habitats_parcels",
    medallion="silver",
    source="cec",
    restricted=True,
    is_geo=False,
    model=CECSoilScapesHabitatsParcels,
    dependencies=[cec_soilscapes_parcels],
    func=_join_habitat_types,
)
"""Cranfield Environment Centre (CEC) SoilScapes data joined to RPA parcels and
to habitat types, using EVAST's soil type to habitat mapping.
"""
