"""Reference Parcels from Rural Payments Agency.

The parcels came in two files - one with SBIs and one without.
It is unclear why a parcel would not have an SBI - perhaps they are non-agricultural,
however to claim CS you need an SBI so unsure here at present.
"""
import geopandas as gpd
import pandas as pd
from pandera import DataFrameModel, Field
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceDataset


class ReferenceParcelsRaw(DataFrameModel):
    """Model for raw version of Rural Payment Agency's Reference Parcels dataset.

    "More columns may be present in the data and would be persisted but we have defined here the ones we want to validate

    Attributes:
        geometry: The parcel geospatial polygons in EPSG:27700.
    """

    geometry: Geometry(crs=SRID) = Field(coerce=True)


class ReferenceParcelsRawNoSBI(DataFrameModel):
    """Model for raw version of Rural Payment Agency's Reference Parcels dataset.

    "More columns may be present in the data and would be persisted but we have defined here the ones we want to validate

    Attributes:
        geometry: The parcel geospatial polygons in EPSG:27700.
    """

    geometry: Geometry(crs=SRID) = Field(coerce=True, nullable=True)


class ReferenceParcels(DataFrameModel):
    """Model for cleaned version of Rural Payment Agency's Reference Parcels dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        sbi: The single business identifier (SBI), stored as a string.
        area_ha: The area of the parcel in hectares.
        geometry: The parcel geospatial polygons in EPSG:27700.
    """

    id_parcel: str = Field(coerce=True, str_matches=r"(^[A-Z]{2}[\d]{8}$)", unique=True)
    sbi: str = Field(coerce=True, str_matches=r"(^[\d]{9}$)", nullable=True)
    area_ha: float = Field(coerce=True, gt=0)
    geometry: Geometry(crs=SRID) = Field(coerce=True)


def _combine_and_clean_parcels(parcels_sbi: Dataset, parcels_nosbi: Dataset) -> gpd.GeoDataFrame:
    """Combine SBI and non SBI parcels and clean up the geometries.

    Info:
        What this is doing:
        - Concatenate the SBI and non SBI datasets
        - Rename PARCEL_ID to id_parcel to align to our modelling standards
        - Make the geometries valid
        - Explode the geometries to unpack them as they are all MultiPolygons
        - Filter out some LineStrings to keep only Polygons
        - Remove any polygons less than 50m2. There are some erroneous shapes in there. This won't remove parcels as the smallest parcel is 100m2.
        - Dissolve to join any parcels that are MultiPolygons back together. There is only one MultiPolygon (TL86476269).
        - Simplify the geometries to 1m resolution to reduce the mean coordinates from 92 per parcel to 21.
        - Set the precision of the geometries to 1m (snap to 1m grid) to avoid floating point errors and enable integer calcs if desired.
        - Remove any repeated points from the precision reduction.
        - Make the geometries valid again.

    Note:
        Simplification effects at different tolerances:

        - Before: mean points=92, total area= 9,780,243 ha
        - **1m simplification: mean points=21, total area= 9,779,732 ha**
        - 5m simplification: mean points=11, total area= 9,770,628 ha
        - 10m simplification: mean points=8, total area= 9,752,380 ha
    """
    return (
        pd.concat(
            [
                parcels_sbi.gdf(),
                parcels_nosbi.gdf().loc[lambda df: df.geometry.notna(), :],
            ]
        )
        .rename(columns=dict(SBI="sbi", FULL_PARCEL_ID="id_parcel"))
        .drop(columns=["PARCEL_ID", "SHAPE_Length", "SHAPE_Area", "SHEET_ID", "GEOM_AR_HA"])
        .loc[:, ["id_parcel", "sbi", "geometry"]]
        .assign(geometry=lambda df: df.geometry.make_valid())
        .explode()
        .loc[lambda df: (df.geometry.geometry.type == "Polygon") & (df.area > 50.0)]
        .dissolve(by="id_parcel")
        .assign(geometry=lambda df: df.geometry.simplify(1).set_precision(0).remove_repeated_points(1).make_valid())
        .explode()
        .loc[lambda df: (df.geometry.geometry.type == "Polygon") & (df.area > 50.0)]
        .dissolve(by="id_parcel")
        .assign(area_ha=lambda df: df.geometry.area / 10_000)
        .reset_index()
    )


reference_parcels_raw = SourceDataset(
    name="reference_parcels_raw",
    level0="bronze",
    level1="rural_payments_agency",
    model=ReferenceParcelsRaw,
    restricted=False,
    source_path="/dbfs/mnt/lab/unrestricted/elm_data/rpa/reference_parcels/LIDM_Parcels.gdb",
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
