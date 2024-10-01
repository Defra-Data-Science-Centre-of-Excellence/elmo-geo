"""Peatland datasets from Defra, supplied by DASH.

Peaty Soils is a simplified England Peat Status GHG and C storage (Peat Layer).
Peat Layer contains many more features and columns than Peaty Soils, but they are the same total area to 1ha.

[^DASH: Peaty Soils]: https://app.powerbi.com/groups/de0d7293-1d23-4194-869d-a4ff2ed2d169/reports/c8802134-4f3b-484e-bf14-1ed9f8881450?ctid=770a2450-0227-4c62-90c7-4e38537f1102&pbi_source=linkShare&bookmarkGuid=093de660-dc9d-4bf4-bf82-deeb3aa69dc6
[^DASH: Search "peat"]: https://app.powerbi.com/groups/de0d7293-1d23-4194-869d-a4ff2ed2d169/reports/c8802134-4f3b-484e-bf14-1ed9f8881450?ctid=770a2450-0227-4c62-90c7-4e38537f1102&pbi_source=linkShare&bookmarkGuid=efe914ce-17ad-44c4-b3d5-21a5f07e7929
"""
from functools import partial

import geopandas as gpd
import pyspark.sql.functions as F
from pandera import DataFrameModel, Field
from pandera.dtypes import Int32
from pandera.engines.geopandas_engine import Geometry
from pyspark.sql.types import BinaryType, StringType, StructField, StructType

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import sjoin_parcel_proportion, sjoin_parcels
from elmo_geo.st.geometry import load_geometry
from elmo_geo.utils.types import PandasDataFrame

from .os import os_bng_raw
from .rpa_reference_parcels import reference_parcels


class PeatySoilsRaw(DataFrameModel):
    """Model for Defra Peaty Soils (2008) dataset.

    Attributes:
        fid: feature id.
        group: peatland class description:
            Deep Peaty Soils
            Shallow Peaty Soils
            Soils with Peaty Pockets
        geometry: Polygon geometries in EPSG:27700.
    """

    fid: Int32 = Field(unique=True, alias="objectid")
    group: str = Field(alias="pclassdesc", isin=["Deep Peaty Soils", "Shallow Peaty Soils", "Soils with Peaty Pockets"])
    geometry: Geometry(crs=SRID) = Field()


peaty_soils_raw = SourceDataset(
    name="peaty_soils_raw",
    level0="bronze",
    level1="defra",
    model=PeatySoilsRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_defra_data_services_platform/dataset_peaty_soils/format_GEOPARQUET_peaty_soils/LATEST_peaty_soils/refdata_owner/",
)


class PeatySoilsParcels(DataFrameModel):
    """Model for Defra Peaty Soils with parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        group: peatland class description.
        proportion: proportion of Parcel Geometry(crs=SRID) overlapping with feature geometry.
    """

    id_parcel: str = Field()
    group: str = Field()
    proportion: float = Field(ge=0, le=1)


peaty_soils_parcels = DerivedDataset(
    is_geo=False,
    name="peaty_soils_parcels",
    level0="silver",
    level1="defra",
    restricted=False,
    func=partial(sjoin_parcel_proportion, columns=["group"]),
    dependencies=[reference_parcels, peaty_soils_raw],
    model=PeatySoilsParcels,
)


def _sjoin_bng_to_no_peat_parcel(
    reference_parcels: Dataset,
    peaty_soils_raw: Dataset,
    os_bng_raw: Dataset,
) -> PandasDataFrame:
    """Produces dataframe linking parcels to 1km BNG tiles.

    Combines parcels, peat and OS BNG geometries to identify the proportion of non-peat parcel area
    intersected by different 1km BNG grid tiles.

    These proportions are used to aggregate ESC M3 outputs to parcel level, since the ESC-CARBINE model
    outputs results for 1km grid tiles.
    """

    schema = StructType([StructField("id_parcel", StringType(), True), StructField("geometry", BinaryType(), True)])

    def _udf_difference(pdfs):
        """Get the parcel geometry excluding peaty soils"""
        for pdf in pdfs:
            yield (
                pdf.assign(geometry=gpd.GeoSeries.from_wkb(pdf["geometry_left"]).difference(gpd.GeoSeries.from_wkb(pdf["geometry_right"])).to_wkb()).reindex(
                    columns=["id_parcel", "geometry"]
                )
            )

    # parcels intersecting peat
    sdf_peat = (
        reference_parcels.sdf()
        .select("id_parcel", "geometry")
        .transform(sjoin_parcels, peaty_soils_raw.sdf().withColumn("geometry", load_geometry(encoding_fn="")))
        .selectExpr(
            "id_parcel",
            "ST_AsBinary(geometry_left) as geometry_left",
            "ST_AsBinary(geometry_right) as geometry_right",
        )
        .mapInPandas(_udf_difference, schema=schema)
        .withColumn("geometry", load_geometry())
        .withColumn("nopeat_area", F.expr("ST_Area(geometry)/10000"))
    )

    # all parcels
    sdf_other = (
        reference_parcels.sdf()
        .join(sdf_peat.select("id_parcel", "nopeat_area"), on="id_parcel", how="left")
        .filter("nopeat_area IS NULL")
        .selectExpr("id_parcel", "geometry", "area_ha as nopeat_area")
    )

    return sjoin_parcel_proportion(sdf_other.unionByName(sdf_peat), os_bng_raw.sdf().filter("layer='1km_grid'"), columns=["tile_name", "nopeat_area"])


class NoPeatParcelBNGModel(DataFrameModel):
    """Proportion of no-peat parcel geometries intersected by BNG 1km grids.

    Attributes:
        id_parcel: Parcel ID
        nopeat_area: Geographic area of parcel excluding intersecting peaty soils geometries.
        tile_name: Name of 1km OS BNG tile intersected the parcel.
        proportion: Proportion of parcel no peat area intersected by 1km BNG tile.
    """

    id_parcel: str = Field()
    nopeat_area: float = Field()
    tile_name: str = Field()
    proportion: float = Field(ge=0, le=1)


os_bng_no_peat_parcels = DerivedDataset(
    name="os_bng_no_peat_parcels",
    level0="silver",
    level1="os",
    restricted=False,
    is_geo=False,
    dependencies=[
        reference_parcels,
        peaty_soils_raw,
        os_bng_raw,
    ],
    func=_sjoin_bng_to_no_peat_parcel,
    model=NoPeatParcelBNGModel,
)
"""Removing peaty soils from RPA parcels and then calculating overlap with BNG 1km grid.
"""
