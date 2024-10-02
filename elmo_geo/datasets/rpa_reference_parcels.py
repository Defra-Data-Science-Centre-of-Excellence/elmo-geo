"""Reference Parcels from Rural Payments Agency.

The parcels came in two files - one with SBIs and one without.
It is unclear why a parcel would not have an SBI - perhaps they are non-agricultural,
however to claim CS you need an SBI so unsure here at present.
"""
import geopandas as gpd
import pandas as pd
from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceDataset


class ReferenceParcelsRaw(DataFrameModel):
    """Model for raw version of Rural Payment Agency's Reference Parcels dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        sbi: The single business identifier (SBI), stored as a string.
        area_ha: The original area of the parcel in hectares.
        geometry: The parcel geospatial polygons in EPSG:27700.
    """

    id_parcel: str = Field(str_matches=r"(^[A-Z]{2}[\d]{8}$)", unique=True, alias="FULL_PARCEL_ID")
    sbi: str = Field(str_matches=r"(^[\d]{9}$)", alias="SBI")
    area_ha: float = Field(gt=0, alias="GEOM_AR_HA")
    geometry: Geometry(crs=SRID) = Field()


class ReferenceParcelsRawNoSBI(DataFrameModel):
    """Model for raw version of Rural Payment Agency's Reference Parcels dataset without any SBI.
    This dataset is messy containing null values instead of geometries, all SBIs should be null, and id_parcel can be duplicated.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        area_ha: The original area of the parcel in hectares.
        geometry: The parcel geospatial polygons in EPSG:27700 but not recorded as such due to null geometries.
    """

    id_parcel: str = Field(str_matches=r"(^[A-Z]{2}[\d]{8}$)", alias="FULL_PARCEL_ID", nullable=True)
    sbi: str = Field(str_matches=r"(^[\d]{9}$)", alias="SBI", nullable=True)
    area_ha: float = Field(gt=0, alias="GEOM_AR_HA", nullable=True)
    geometry: Geometry(crs=None) = Field(nullable=True)


class ReferenceParcels(DataFrameModel):
    """Model for cleaned version of Rural Payment Agency's Reference Parcels dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        sbi: The single business identifier (SBI), stored as a string.
        area_ha: The original area of the parcel in hectares.
        geometry: The parcel geospatial polygons in EPSG:27700.
    """

    id_parcel: str = Field(str_matches=r"(^[A-Z]{2}[\d]{8}$)", unique=True)
    sbi: str = Field(str_matches=r"(^[\d]{9}$)", nullable=True)
    area_ha: float = Field(gt=0)
    geometry: Geometry(crs=SRID) = Field()


def _combine_and_clean_parcels(parcels_sbi: Dataset, parcels_nosbi: Dataset) -> gpd.GeoDataFrame:
    """Combine SBI and non SBI parcels and clean up the geometries.

    Info:
        What this is doing:
        - Concatenate the SBI and non SBI datasets
        - Make the geometries valid
        - Explode the geometries to unpack them as they are all MultiPolygons
        - Filter out some LineStrings to keep only Polygons
        - Remove any polygons less than 50m2. There are some erroneous shapes in there. This won't remove parcels as the smallest parcel is 100m2.
        - Dissolve to join any parcels that are MultiPolygons back together. There is only one MultiPolygon (TL86476269).
        - Simplify the geometries to 1m resolution to reduce the mean coordinates from 92 per parcel to 21.
        - Set the precision of the geometries to 1m (snap to 1m grid) to avoid floating point errors and enable integer calcs if desired.
        - Remove any repeated points from the precision reduction.
        - Make the geometries valid again.

    Simplification effects at different tolerances:
        Abs Sum: 16,166ha
        Abs Mean: 0.006ha
    ```py
    df = reference_parcels.gdf().assign(diff=lambda df: df.area / 10_000 - df["area_ha"])
    title = f'Parcel change in area cause by Precision 1.\n{df["diff"].abs().sum():,.0f}ha abs sum, {df["diff"].abs().mean():.3f}ha abs mean difference.'
    df["diff"].hist(bins=100).set(title=title)
    ```
    """
    return (
        pd.concat(
            [
                parcels_sbi.gdf(),
                parcels_nosbi.gdf().loc[lambda df: df.geometry.notna(), :],
            ]
        )
        .loc[:, ["id_parcel", "sbi", "area_ha", "geometry"]]
        .assign(geometry=lambda df: df.geometry.make_valid())
        .explode()
        .loc[lambda df: (df.geometry.geometry.type == "Polygon") & (df.area > 50.0)]
        .dissolve(by="id_parcel")
        .assign(geometry=lambda df: df.geometry.simplify(1).set_precision(1).remove_repeated_points(1).make_valid())
        .explode()
        .loc[lambda df: (df.geometry.geometry.type == "Polygon") & (df.area > 50.0)]
        .dissolve(by="id_parcel")
        .reset_index()
    )


reference_parcels_raw = SourceDataset(
    name="reference_parcels_raw",
    level0="bronze",
    level1="rural_payments_agency",
    model=ReferenceParcelsRaw,
    restricted=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/LIDM_Parcels.gdb",
    clean_geometry=False,
)
"""Definition for the raw sourced version of the RPA's Reference Parcels dataset.

Current version is as of 1st June 2023 and is what is being used by us, LEEP WFM and EVAST.
"""

reference_parcels_raw_no_sbi = SourceDataset(
    name="reference_parcels_raw_no_sbi",
    level0="bronze",
    level1="rural_payments_agency",
    model=ReferenceParcelsRawNoSBI,
    restricted=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/No_SBI_Parcels.gdb",
    clean_geometry=False,
)
"""Raw parcels that have no SBI.

Current version is as of 1st June 2023 and is what is being used by us, LEEP WFM and EVAST.
"""

reference_parcels = DerivedDataset(
    name="reference_parcels",
    level0="silver",
    level1="rural_payments_agency",
    model=ReferenceParcels,
    restricted=False,
    func=_combine_and_clean_parcels,
    dependencies=[reference_parcels_raw, reference_parcels_raw_no_sbi],
)
"""Definition for the cleaned version of the RPA's Reference Parcels dataset.

Current version is as of 1st June 2023 and is what is being used by us, LEEP WFM and EVAST.
The parcel geometries have been cleaned and simplified to 1m resolution.
There is one MultiPolygon, the rest are Polygons.
"""
