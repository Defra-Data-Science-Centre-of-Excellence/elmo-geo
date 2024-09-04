"""Cranfield Environment Centre (CEC) SoilScapes data.

The CEC SoilSacpaes dataset is a product derived from the National Soil Map. It seeks to provide a useful, concise, easily interpreted and
applicable description of the soils of England and Wales. It is a simplified rendition of the national soil map and contains about
30 distinct soil types.
"""

from functools import partial

import pandas as pd
from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels

_join_parcels = partial(join_parcels, columns=["unit", "natural_dr", "natural_fe"])


class CECSoilScapesRaw(DataFrameModel):
    """Cranfield Environment Centre (CEC) SoilScapes data model.

    Parameters:
        geometry: Geometry indicating the extent of the soil type.
    """

    geometry: Geometry = Field(coerce=True, nullable=False)


cec_soilscapes_raw = SourceDataset(
    name="cec_soilscapes_raw",
    level0="bronze",
    level1="cec",
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
        proportion: Proportion of the parcel intersected by the soil type.
    """

    # TODO: Alias the field names. Aliasing being added in a separate PR.
    id_parcel: str = Field(nullable=False, coerce=True)
    unit: Category = Field(coerce=True, nullable=False, isin=set(range(1, 32)).difference([29]))
    natural_dr: Category = Field(
        coerce=True,
        nullable=False,
        isin=["Freely draining", "Naturally wet", " ", "Impeded drainage", "Variable", "Slightly impeded drainage", "Surface wetness"],
    )
    natural_fe: Category = Field(
        coerce=True,
        nullable=False,
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
    proportion: float = Field(coerce=True, ge=0, le=1)


cec_soilscapes_parcels = DerivedDataset(
    name="cec_soilscapes_parcels",
    level0="silver",
    level1="cec",
    restricted=True,
    is_geo=False,
    model=CECSoilScapesParcels,
    dependencies=[reference_parcels, cec_soilscapes_raw],
    func=_join_parcels,
)
"""Cranfield Environment Centre (CEC) SoilScapes data joined to RPA parcels.
"""


def _soilscape_habitat_lookup() -> pd.DataFrame:
    """A lookup from Soilscape soil type to habitat types.

    Produced by EVAST, this is a one-to-many lookup from Soilscape soil type to
    habitats. It indicates which habitats can occur on each soil type.
    """
    # TODO: Use complete lookup from soil type to habitats. Blocked by DASH data upload request.
    code_to_name = {
        "UHL": "Upland heathland",
        "LHL": "Lowland heathland",
        "LCG": "Lowland calcareous grassland",
        "LAG": "Lowland dry acid grassland",
        "LMW": "Lowland meadows",
        "UHM": "Upland hay meadows",
        "UCG": "Upland calcareous grassland",
        "LRB": "Lowland raised bog",
        "UFS": "Upland fens",
        "BBG": "Blanket bog",
        "LFN": "Lowland fens",
        "PMG": "Purple moorgrass and rush pasture",
    }
    return (
        pd.DataFrame(
            [
                {"unit": 1},
                {"unit": 2, "ULH": 1, "LHL": 1, "UFS": 1},
                {"unit": 3, "LCG": 1, "UCG": 1},
            ],
            columns=pd.Index(data=["unit", "UHL", "LHL", "LCG", "LAG", "LMW", "UHM", "UCG", "LRB", "UFS", "BBG", "LFN", "PMG"], name="habitat_code"),
        )
        .fillna(0)
        .set_index("unit")
        .stack()
        .reset_index()
        .assign(habitat_name=lambda df: df.habitat_code.replace(code_to_name))
        .drop(0, axis=1)
    )


def _join_habitat_types(soilscapes_parcels: DerivedDataset) -> pd.DataFrame:
    return soilscapes_parcels.pdf().merge(_soilscape_habitat_lookup(), on="unit", how="left", validate="m:m")


class CECSoilScapesParcels(DataFrameModel):
    """Cranfield Environment Centre (CEC) SoilScapes data model.

    Habitat types are as follows:


    Parameters:
        id_parcel: The parcel ID.
        unit: Soil type category, expressed as an integer.
        habitat_code: Abbreviation indiating the habitat type that can exist on the soil type.
        habitat_name: Name of the habitat type that can exist on the soil type.
    """

    id_parcel: str = Field(nullable=False, coerce=True)
    unit: Category = Field(coerce=True, nullable=True, isin=set(range(1, 32)).difference([29]))
    habitat_code: Category = Field(coerce=True, nullable=True, isin=["UHL", "LHL", "LCG", "LAG", "LMW", "UHM", "UCG", "LRB", "UFS", "BBG", "LFN", "PMG"])
    habitat_name: Category = Field(
        coerce=True,
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
        ],
    )


cec_soilscapes_habitats_parcels = DerivedDataset(
    name="cec_soilscapes_habitats_parcels",
    level0="silver",
    level1="cec",
    restricted=True,
    is_geo=False,
    dependencies=[cec_soilscapes_parcels],
    func=_join_habitat_types,
)
"""Cranfield Environment Centre (CEC) SoilScapes data joined to RPA parcels and
to habitat types.
"""
