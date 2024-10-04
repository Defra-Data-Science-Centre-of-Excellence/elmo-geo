"""RPA Land Cover
"""
from pandera import DataFrameModel, Field

from elmo_geo.etl.etl import DerivedDataset, SourceDataset


class RPALandCoverParcelsRaw(DataFrameModel):
    """Model for raw RPA land cover data."""

    FULL_PARCEL_ID: str = Field(alias="id_parcel")
    LAND_COVER_CLASS_CODE: int = Field(alias="land_cover_code", coerce=True)
    LC_AREA: float = Field(alias="area")


rpa_land_cover_parcels_raw = SourceDataset(
    name="rpa_land_cover_parcels_raw",
    level0="bronze",
    level1="rpa",
    model=RPALandCoverParcelsRaw,
    restricted=True,
    source_path="/dbfs/mnt/lab/unrestricted/ELM-Project/raw/LandCovers_Table.gdb",
)


class RPALandCoverCodesRaw(DataFrameModel):
    """Model for RPA land cover codes lookup."""

    LAND_COVER_CLASS_CODE: int = Field(alias="land_cover_code", coerce=True)
    NAME: str = Field(alias="land_cover_name")


rpa_land_cover_codes_raw = SourceDataset(
    name="rpa_land_cover_codes_raw",
    level0="bronze",
    level1="rpa",
    model=RPALandCoverCodesRaw,
    restricted=True,
    is_geo=False,
    source_path="/dbfs/mnt/lab/restricted/ELM-Project/raw/rpa/LOOKUP_LANDCOVERS.csv",
)


class RPALandCoverParcels(DataFrameModel):
    """Model for RPA land cover data with land cover names."""

    id_parcel: str = Field()
    land_cover_code: int = Field(coerce=True)
    land_cover_name: str = Field()
    area: float = Field()
    area_ha: float = Field()


def _transform(rpa_land_cover_parcels, rpa_land_cover_codes):
    return rpa_land_cover_parcels.pdf().merge(rpa_land_cover_codes.pdf(), on="LAND_COVER_CLASS_CODE").assign(area_ha=lambda df: df["area"] / 10_000)


rpa_land_cover_parcels = DerivedDataset(
    name="rpa_land_cover_parcels",
    level0="silver",
    level1="rpa",
    model=RPALandCoverParcels,
    restricted=True,
    is_geo=False,
    dependencies=[rpa_land_cover_parcels_raw, rpa_land_cover_codes_raw],
    func=_transform,
)
