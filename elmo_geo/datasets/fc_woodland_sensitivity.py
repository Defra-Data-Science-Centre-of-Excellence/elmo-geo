"""England Woodland Creation Full Sensitivity Map and it's variants from Forestry Commission.

[A guide to Forestry Commission's sensitivity maps for woodland creation](https://www.gov.uk/guidance/a-guide-to-forestry-commissions-sensitivity-maps-for-woodland-creation)

The low sensitivity areas have fewest identified constraints to address,
and it should be easier to agree creating new woodland here than in other areas.
"""
from functools import partial

import geopandas as gpd
from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels

_join_parcels = partial(join_parcels, columns=["sensitivity"])


class WoodlandSensitivityClean(DataFrameModel):
    """Model describing the Forestry Commission's England Woodland Creation Sensitivity Maps.

    Attributes:
        sensitivity: The sensitivity classification, one of `{Unsuitable, High, Medium, Low}`.
        geometry: The sensitivity classification's geospatial extent (polygons).
    """

    sensitivity: str = Field(isin=["Unsuitable", "High", "Medium", "Low"])
    geometry: Geometry(crs=SRID) = Field()


class WoodlandSensitivityParcels(DataFrameModel):
    """Model describing a woodland creation sensitivity parcel-level dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        sensitivity: The sensitivity classification, one of `{Unsuitable, High, Medium, Low}`.
        proportion: The proportion of the parcel that intersects with the sensitivity classification.
    """

    id_parcel: str = Field()
    sensitivity: str = Field(isin=["Unsuitable", "High", "Medium", "Low"])
    proportion: float = Field(ge=0, le=1)


def _clean_dataset(ds: Dataset) -> gpd.GeoDataFrame:
    """Only keep the geometry and the sensitivity col, fixing typo in colname."""
    return ds.gdf(columns=["geometry", "sensitivit"]).rename(columns={"sensitivit": "sensitivity"}).assign(fid=lambda df: range(len(df)))


sfi_agroforestry_raw = SourceDataset(
    name="sfi_agroforestry_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/forestry_commission/sfi_agroforestry/2024_04_15/SFI_Agroforestry.shp",
)
"""Definition for the raw sourced version of Forestry Commission's SFI Agroforestry dataset.

Source:
    - [Forestry Commission Open Data](https://data-forestry.opendata.arcgis.com/datasets/94c16efc5c9d47ed8d6634a8d538d166_0)
"""

woodland_creation_sensitivity_raw = SourceDataset(
    name="woodland_creation_sensitivity_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/forestry_commission/woodland_creation_full_sensitivity/England_Woodland_Creation_Full_Sensitivity_Map_v4.0.shp",
)
"""Definition for the raw sourced version of Forestry Commission's England Woodland Creation Full Sensitivity Map.

Source:
    - [Forestry Commission Open Data](https://data-forestry.opendata.arcgis.com/datasets/ffc2d647a1934f75914a9a45bc74c86d_0)
"""

woodland_creation_sensitivity_var1_raw = SourceDataset(
    name="woodland_creation_sensitivity_var1_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/forestry_commission/woodland_creation_full_sensitivity_variant_1/England_Woodland_Creation_Full_Sensitivity_Map_v4.0_variant_1.shp",
)
"""Definition for the raw sourced version of Forestry Commission's England Woodland Creation Full Sensitivity Map Variant 1.

- Agricultural land class 3a: changed to low sensitivity
- Protected landscapes (National Parks and National Landscapes): changed to low sensitivity

Source:
    - [Forestry Commission Open Data](https://data-forestry.opendata.arcgis.com/maps/018eb6b8bb4b423c92c6afde72c7f700)
"""

woodland_creation_sensitivity_var2_raw = SourceDataset(
    name="woodland_creation_sensitivity_var2_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/forestry_commission/woodland_creation_full_sensitivity_variant_2/England_Woodland_Creation_Full_Sensitivity_Map_v4.0_variant_2.shp",
)
"""Definition for the raw sourced version of Forestry Commission's England Woodland Creation Full Sensitivity Map Variant 2.

- Agricultural land class 3a: remaining medium sensitivity
- Protected landscapes: changed to low sensitivity

Source:
    - [Forestry Commission Open Data](https://data-forestry.opendata.arcgis.com/datasets/eddd19ee195b4471bc868856fbe63afe_0)
"""

woodland_creation_sensitivity_var3_raw = SourceDataset(
    name="woodland_creation_sensitivity_var3_raw",
    level0="bronze",
    level1="forestry_commission",
    restricted=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/forestry_commission/woodland_creation_full_sensitivity_variant_3/England_Woodland_Creation_Full_Sensitivity_Map_v4.0_variant_3.shp",
)
"""Definition for the raw sourced version of Forestry Commission's England Woodland Creation Full Sensitivity Map Variant 3.

Source:
    - [Forestry Commission Open Data](https://data-forestry.opendata.arcgis.com/datasets/ffc2d647a1934f75914a9a45bc74c86d_0)
"""

sfi_agroforestry = DerivedDataset(
    name="sfi_agroforestry",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    model=WoodlandSensitivityClean,
    func=_clean_dataset,
    dependencies=[sfi_agroforestry_raw],
)
"""Definition for the cleaned version of Forestry Commission's SFI Agroforestry dataset.

Columns have been renamed and dropped from the daw-version but the data/rows remain consistent.
"""

woodland_creation_sensitivity = DerivedDataset(
    name="woodland_creation_sensitivity",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    model=WoodlandSensitivityClean,
    func=_clean_dataset,
    dependencies=[woodland_creation_sensitivity_raw],
)
"""Definition for the cleaned version of Forestry Commission's England Woodland Creation Full Sensitivity Map.

Columns have been renamed and dropped from the raw-version but the data/rows remain consistent.
"""

woodland_creation_sensitivity_var1 = DerivedDataset(
    name="woodland_creation_sensitivity_var1",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    model=WoodlandSensitivityClean,
    func=_clean_dataset,
    dependencies=[woodland_creation_sensitivity_var1_raw],
)
"""Definition for the cleaned version of Forestry Commission's England Woodland Creation Full Sensitivity Map Variant 1.

Columns have been renamed and dropped from the raw-version but the data/rows remain consistent.
"""

woodland_creation_sensitivity_var2 = DerivedDataset(
    name="woodland_creation_sensitivity_var2",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    model=WoodlandSensitivityClean,
    func=_clean_dataset,
    dependencies=[woodland_creation_sensitivity_var2_raw],
)
"""Definition for the cleaned version of Forestry Commission's England Woodland Creation Full Sensitivity Map Variant 2.

Columns have been renamed and dropped from the raw-version but the data/rows remain consistent.
"""

woodland_creation_sensitivity_var3 = DerivedDataset(
    name="woodland_creation_sensitivity_var3",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    model=WoodlandSensitivityClean,
    func=_clean_dataset,
    dependencies=[woodland_creation_sensitivity_var3_raw],
)
"""Definition for the cleaned version of Forestry Commission's England Woodland Creation Full Sensitivity Map Variant 3.

Columns have been renamed and dropped from the raw-version but the data/rows remain consistent.
"""

sfi_agroforestry_parcels = DerivedDataset(
    name="sfi_agroforestry_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=_join_parcels,
    dependencies=[reference_parcels, sfi_agroforestry],
    model=WoodlandSensitivityParcels,
    is_geo=False,
)
"""Definition for Forestry Commission's SFI Agroforestry dataset joined to RPA Parcels."""

woodland_creation_sensitivity_parcels = DerivedDataset(
    name="woodland_creation_sensitivity_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=_join_parcels,
    dependencies=[reference_parcels, woodland_creation_sensitivity],
    model=WoodlandSensitivityParcels,
    is_geo=False,
)
"""Definition for the Forestry Commission's England Woodland Creation Full Sensitivity Map joined to RPA Parcels."""

woodland_creation_sensitivity_var1_parcels = DerivedDataset(
    name="woodland_creation_sensitivity_var1_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=_join_parcels,
    dependencies=[reference_parcels, woodland_creation_sensitivity_var1],
    model=WoodlandSensitivityParcels,
    is_geo=False,
)
"""Definition for the Forestry Commission's England Woodland Creation Full Sensitivity Map Variant 1 joined to RPA Parcels."""

woodland_creation_sensitivity_var2_parcels = DerivedDataset(
    name="woodland_creation_sensitivity_var2_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=_join_parcels,
    dependencies=[reference_parcels, woodland_creation_sensitivity_var2],
    model=WoodlandSensitivityParcels,
    is_geo=False,
)
"""Definition for the Forestry Commission's England Woodland Creation Full Sensitivity Map Variant 2 joined to RPA Parcels."""

woodland_creation_sensitivity_var3_parcels = DerivedDataset(
    name="woodland_creation_sensitivity_var3_parcels",
    level0="silver",
    level1="forestry_commission",
    restricted=False,
    func=_join_parcels,
    dependencies=[reference_parcels, woodland_creation_sensitivity_var3],
    model=WoodlandSensitivityParcels,
    is_geo=False,
)
"""Definition for the Forestry Commission's England Woodland Creation Full Sensitivity Map Variant 3 joined to RPA Parcels."""
