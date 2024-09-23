"""RPA Less Favourable Areas (LFA) and Moorland Line (Moorline), provided by DASH.

In combination this defines "uplands", as either Less Favourable Areas (LFA) areas and/or Moorland areas:
- D: Disadvantaged Area
- S: Severely Disadvantaged Area
- MD: Moorland and Disadvantaged Area
- MS: Moorland and Severely Disadvantaged Area

[^DASH: Moorline]: https://app.powerbi.com/groups/de0d7293-1d23-4194-869d-a4ff2ed2d169/reports/c8802134-4f3b-484e-bf14-1ed9f8881450?ctid=770a2450-0227-4c62-90c7-4e38537f1102&pbi_source=linkShare&bookmarkGuid=d4890c8c-e768-4eb7-9c5d-53ffbf332339
[^Gov Data: LFA and Moorland]: https://www.data.gov.uk/dataset/0817bc9e-341f-4d8c-be66-38b1fab69b21/
"""

from functools import partial

import pyspark.sql.functions as F
from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.geopandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels
from elmo_geo.utils.types import PandasDataFrame

from .rpa_reference_parcels import reference_parcels


class MoorlineRaw(DataFrameModel):
    """Model for Moorline dataset.

    Attributes:
        name: Upland classification:
            D: Disadvantaged Area
            S: Severely Disadvantaged Area
            MD: Moorland and Disadvantaged Area
            MS: Moorland and Severely Disadvantaged Area
        geometry: (Multi)Polygon geometries in EPSG:27700.
    """

    name: Category = Field(isin=["D", "S", "MD", "MS"])
    geometry: Geometry(crs=SRID) = Field()


moorline_raw = SourceDataset(
    name="moorline_raw",
    level0="bronze",
    level1="rpa",
    model=MoorlineRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_lfa_and_moorland_line/format_GEOPARQUET_lfa_and_moorland_line/LATEST_lfa_and_moorland_line/",
)


class MoorlineParcel(DataFrameModel):
    """Model for Moorline joined to parcels.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        name: Upland classification; D, S, MD, MS.
        proportion: proportion of Parcel Geometry(crs=SRID) overlapping with feature geometry.
    """

    id_parcel: str = Field()
    name: str = Field()
    proportion: float = Field(ge=0, le=1)


moorline_parcels = DerivedDataset(
    is_geo=False,
    name="moorline_parcels",
    level0="silver",
    level1="rpa",
    restricted=False,
    func=partial(join_parcels, columns=["name"]),
    dependencies=[reference_parcels, moorline_raw],
    model=MoorlineParcel,
)


class IsUplandParcel(DataFrameModel):
    """Model for 'is upland' parcel classification..

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        is_upland: Boolean indicating whether largest intersecting lfa feature is moorland.
    """

    id_parcel: str = Field()
    is_upland: bool = Field()


def _is_upland(reference_parcels: DerivedDataset, moorline_parcels: DerivedDataset) -> PandasDataFrame:
    """Classify each parcel as upland or not."""
    return (
        reference_parcels.sdf()
        .select("id_parcel")
        .join(moorline_parcels.sdf(), on="id_parcel", how="left")
        .orderBy("proportion", ascending=False)
        .groupby("id_parcel")
        .agg(F.expr("FIRST(name) as name"))
        .selectExpr("id_parcel", "COALESCE(name in ('MD', 'MS'), FALSE) AS is_upland")
    ).toPandas()


is_upland_parcels = DerivedDataset(
    is_geo=False,
    name="is_upland_parcels",
    level0="silver",
    level1="rpa",
    restricted=False,
    func=_is_upland,
    dependencies=[reference_parcels, moorline_parcels],
    model=IsUplandParcel,
)
