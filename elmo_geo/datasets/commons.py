"""Commons datasets from Defra and RPA, supplied by DASH.

Common Land Amalgamation merges Registered Common Land (BPS Layer), CRoW Act 2000 Section 4, Historic Common Land.

[^DASH Data Catalogue]: https://app.powerbi.com/groups/me/apps/5762de14-3aa8-4a83-92b3-045cc953e30c/reports/c8802134-4f3b-484e-bf14-1ed9f8881450/ReportSectionff2a0c223272005d9b10?ctid=770a2450-0227-4c62-90c7-4e38537f1102&experience=power-bi
"""
from functools import partial

from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels


class CommonsRaw(DataFrameModel):
    """Model for RPA Common Land Amalgamation dataset.

    Attributes:
        name: Description or name of the common
        source: Source of the data
            "BPS_RCL" this is Registered Common Land (BPS Layer), this is conclusively common land.
            "CROW" this is CRoW Act 2000 Section 4, this is conclusively common land.
            "BPS_RCL & CROW", this is conclusively common land.
            "HISTORIC2001?" this if from many historic source that make Historic Common Land, but not found in BPS or CRoW, and is unconclusively common land.
        geometry: Polygon geometries in EPSG:27700.
    """

    name: str = Field(coerce=True)
    source: Category = Field(coerce=True, isin=["BPS_RCL", "BPS_RCL & CROW", "CROW", "HISTORIC2001?"])
    geometry: Geometry = Field(coerce=True)


commons_raw = SourceDataset(
    name="commons_raw",
    level0="bronze",
    level1="defra",
    model=CommonsRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_rpa_spatial_data_mart/dataset_common_land_amalgamation/format_GPKG_common_land_amalgamation/LATEST_common_land_amalgamation/refdata_owner.common_land_amalgamation.zip/refdata_owner.common_land_amalgamation/refdata_owner.common_land_amalgamation.gpkg",
)


class CommonsParcels(DataFrameModel):
    """Model for Defra ALC with parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        conclusive: Is this area conclusively common land, or only suggested by historic data sources.
        proportion: proportion of Parcel geometry overlapping with feature geometry.
    """

    id_parcel: str = Field()
    conclusive: bool = Field()  # TODO: gdf["conclusive"] = gdf["source"] in ['CROW', 'BPS_RCL', 'BPS_RCL & CROW']
    proportion: float = Field(ge=0, le=1)


commons_parcels = DerivedDataset(
    name="commons_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    func=partial(join_parcels, columns=["conclusive"]),
    dependencies=[reference_parcels, commons_raw],
    model=CommonsParcels,
)
