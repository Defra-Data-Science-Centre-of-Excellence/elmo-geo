"""RPA parcel lookup to BNG grid tiles and associated geometry geojson.

Used for aggregating and mapping.
"""

import geopandas as gpd
import pandas as pd
from pandera import DataFrameModel, Field
from pandera.dtypes import Int32
from pyspark.sql.functions import pandas_udf
from shapely import to_geojson

from elmo_geo.etl import Dataset, DerivedDataset

from .os import os_bng_raw
from .rpa_reference_parcels import reference_parcels


class ReferenceParcelsGeojson(DataFrameModel):
    """Model for reference parcels to BNG geojson lookup.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        sbi: The single business identifier (SBI), stored as a string.
        x: Parcel centroid x coordinate in OSGB grid reference system (epsg 27700).
        y: Parcel centroid y coordinate in OSGB grid reference system (epsg 27700).
        tile_*km: OSGB tile parcel looks up to at 1km, 10km, and 100km resolutions. Based on parcel sheet ID.
        geojson_*km: OSGB tile geojson at 1km, 10km, and 100km resolutions. In WSG84 (epsg 4326) grid reference.
    """

    id_parcel: str = Field(str_matches=r"(^[A-Z]{2}[\d]{8}$)", unique=True)
    sbi: str = Field(str_matches=r"(^[\d]{9}$)", nullable=True)
    x: Int32 = Field()
    y: Int32 = Field()
    geojson_1km: str = Field()
    tile_1km: str = Field()
    geojson_10km: str = Field()
    tile_10km: str = Field()
    geojson_100km: str = Field()
    tile_100km: str = Field()


def _to_geojson_udf(col):
    @pandas_udf("string")
    def udf(s: pd.Series) -> pd.Series:
        return gpd.GeoSeries.from_wkb(s, crs=27700).to_crs(4326).map(to_geojson)

    return udf(col)


def _parcel_to_bng_geojson_lookup(parcels: Dataset, os_bng_raw: Dataset) -> gpd.GeoDataFrame:
    """Joins parcels to BNG gris based on parcel sheet ID and returns geosjon of grid tiles.

    Produces a dataset suitable for aggregating and mapping parcels outputs at various BNG
    grid resolutions.
    """
    return (
        reference_parcels.sdf()
        .selectExpr("id_parcel", "sbi", "LEFT(id_parcel, 6) as sheet_id", "ST_Centroid(geometry) as centroid")
        .selectExpr(
            "id_parcel",
            "sbi",
            "CAST(ST_X(centroid) AS Int) as x",
            "CAST(ST_Y(centroid) AS Int) as y",
            "sheet_id as tile_1km",
            "CONCAT(LEFT(sheet_id, 3), LEFT(RIGHT(sheet_id, 2), 1)) as tile_10km",
            "LEFT(sheet_id, 2) as tile_100km",
        )
        .join(
            os_bng_raw.sdf().filter("layer='1km_grid'").selectExpr("tile_name as tile_1km", "ST_AsBinary(geometry) as geometry_1km"), on="tile_1km", how="left"
        )
        .join(
            os_bng_raw.sdf().filter("layer='10km_grid'").selectExpr("tile_name as tile_10km", "ST_AsBinary(geometry) as geometry_10km"),
            on="tile_10km",
            how="left",
        )
        .join(
            os_bng_raw.sdf().filter("layer='100km_grid'").selectExpr("tile_name as tile_100km", "ST_AsBinary(geometry) as geometry_100km"),
            on="tile_100km",
            how="left",
        )
        .select(
            "id_parcel",
            "sbi",
            "x",
            "y",
            "tile_1km",
            "tile_10km",
            "tile_100km",
            *[_to_geojson_udf(f"geometry_{i}km").alias(f"geojson_{i}km") for i in [1, 10, 100]],
        )
    )


reference_parcels_bng_geojson = DerivedDataset(
    name="reference_parcels_bng_geojson",
    level0="silver",
    level1="rural_payments_agency",
    model=ReferenceParcelsGeojson,
    restricted=False,
    is_geo=False,
    func=_parcel_to_bng_geojson_lookup,
    dependencies=[reference_parcels, os_bng_raw],
)
"""RPA parcels with lookup to 1km, 10km, and 100km BNG grid tile IDs and geojson geometries.

Used for aggregating and mapping parcel level outputs from elmo. Geojson geometries are in WGS84
(epsg 4326).
"""
