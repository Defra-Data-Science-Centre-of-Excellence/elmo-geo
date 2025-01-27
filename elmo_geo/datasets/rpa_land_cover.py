"""RPA Land Cover
"""
import pyspark.sql.functions as F
from pandera import DataFrameModel, Field

from elmo_geo.etl.etl import DerivedDataset, SourceDataset


class RPALandCoverParcelsRaw(DataFrameModel):
    """Model for raw RPA land cover data."""

    id_parcel: str = Field(alias="FULL_PARCEL_ID")
    land_cover_code: int = Field(alias="LAND_COVER_CLASS_CODE", coerce=True)
    area: float = Field(alias="LC_AREA")


rpa_land_cover_parcels_raw = SourceDataset(
    name="rpa_land_cover_parcels_raw",
    medallion="bronze",
    source="rural_payments_agency",
    model=RPALandCoverParcelsRaw,
    restricted=True,
    source_path="/dbfs/mnt/lab/unrestricted/ELM-Project/raw/LandCovers_Table.gdb",
)


class RPALandCoverCodesRaw(DataFrameModel):
    """Model for RPA land cover codes lookup."""

    land_cover_code: int = Field(alias="LAND_COVER_CLASS_CODE", coerce=True)
    land_cover_name: str = Field(alias="NAME")


rpa_land_cover_codes_raw = SourceDataset(
    name="rpa_land_cover_codes_raw",
    medallion="bronze",
    source="rural_payments_agency",
    model=RPALandCoverCodesRaw,
    restricted=True,
    is_geo=False,
    source_path="/dbfs/mnt/lab-res-a1001004/restricted/elm_project/raw/rpa/LOOKUP_LANDCOVERS.csv",
)


class RPALandCoverParcels(DataFrameModel):
    """Model for RPA land cover data with land cover names."""

    id_parcel: str = Field()
    land_cover_codes: str = Field()
    land_cover_name: str = Field()
    area: float = Field()
    area_ha: float = Field()


def _transform(rpa_land_cover_parcels_raw, rpa_land_cover_codes_raw):
    """Joins RPA parcel land cover area values to land cover names.

    Land cover codes 130 and 131 both have the name 'Permanent Grassland' and
    codes 520 and 525 both have the name 'Structure'. This is why codes have been concatenated
    as part of the groupby aggregation.
    """
    return (
        rpa_land_cover_parcels_raw.sdf()
        .join(rpa_land_cover_codes_raw.sdf(), on="land_cover_code")
        .withColumn("land_cover_name", F.expr("TRIM(land_cover_name)"))
        .groupby("id_parcel", "land_cover_name")
        .agg(
            F.expr("ARRAY_JOIN(SORT_ARRAY(COLLECT_SET(land_cover_code)), ',') AS land_cover_codes"),
            F.expr("SUM(area) as area"),
            F.expr("SUM(area)/10000 as area_ha"),
        )
        .toPandas()
    )


rpa_land_cover_parcels = DerivedDataset(
    name="rpa_land_cover_parcels",
    medallion="silver",
    source="rural_payments_agency",
    model=RPALandCoverParcels,
    restricted=True,
    is_geo=False,
    dependencies=[rpa_land_cover_parcels_raw, rpa_land_cover_codes_raw],
    func=_transform,
)
