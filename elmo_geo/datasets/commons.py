"""Commons datasets from Defra and RPA, supplied by DASH.

Common Land Amalgamation merges Registered Common Land (BPS Layer), CRoW Act 2000 Section 4, Historic Common Land.

[^DASH: Search "common"]: https://app.powerbi.com/groups/de0d7293-1d23-4194-869d-a4ff2ed2d169/reports/c8802134-4f3b-484e-bf14-1ed9f8881450?ctid=770a2450-0227-4c62-90c7-4e38537f1102&pbi_source=linkShare&bookmarkGuid=b7ecc75d-5130-4fb5-8518-86bcb6161ca4
"""
from functools import partial

from pandera import DataFrameModel, Field
from pandera.engines.geopandas_engine import Geometry
from pyspark.sql import functions as F

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import sjoin_parcel_proportion
from elmo_geo.utils.types import SparkDataFrame

from .rpa_reference_parcels import reference_parcels


class CommonsRaw(DataFrameModel):
    """Model for RPA Common Land Amalgamation dataset.

    Attributes:
        name: Description or name of the common
        source: Source of the data
            "BPS_RCL" this is Registered Common Land (BPS Layer).
            "CROW" this is CRoW Act 2000 Section 4.
            "BPS_RCL & CROW".
            "HISTORIC2001?", is from many historic source that make Historic Common Land, but the feature area is not found in BPS or CRoW.
                This is the only dataset classed as inconclusive common land.
        geometry: Polygon geometries in EPSG:27700.
    """

    name: str = Field()
    source: str = Field(isin=["BPS_RCL", "BPS_RCL & CROW", "CROW", "HISTORIC2001?"])
    geometry: Geometry(crs=SRID) = Field()


commons_raw = SourceDataset(
    name="commons_raw",
    medallion="bronze",
    source="defra",
    model=CommonsRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_common_land_amalgamation/format_GEOPARQUET_common_land_amalgamation/LATEST_common_land_amalgamation/refdata_owner/",
)


def fn_pre_conclusive(sdf: SparkDataFrame) -> SparkDataFrame:
    return sdf.withColumn("conclusive", F.col("source").isin(["BPS_RCL", "BPS_RCL & CROW", "CROW"])).drop("source")


class CommonsParcels(DataFrameModel):
    """Model for Defra ALC with parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        conclusive: Is this area conclusively common land, or only suggested by historic data sources.
        proportion: proportion of Parcel geometry overlapping with feature geometry.
    """

    id_parcel: str = Field()
    conclusive: bool = Field()
    proportion: float = Field(ge=0, le=1)


commons_parcels = DerivedDataset(
    is_geo=False,
    name="commons_parcels",
    medallion="silver",
    source="defra",
    restricted=False,
    func=partial(sjoin_parcel_proportion, columns=["conclusive"], fn_pre=fn_pre_conclusive),
    dependencies=[reference_parcels, commons_raw],
    model=CommonsParcels,
)
