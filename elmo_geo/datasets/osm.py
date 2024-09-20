"""OpenStreetMap, provided by DASH.
OSM[^wiki] is an opensource alternative to complete mapping databases like Ordnance Survey NGD,
and often used as an alternative to routing tools like Google Maps[^map].
This is a supply of planet.osm subsetted to United Kingdom and omitting personal data.
OSM is crowd sourced data and continuously updated, some features are well sourced and quickly updated, but this is not the case for all feature types.
[^wiki]: https://wiki.openstreetmap.org/
[^map]: https://www.openstreetmap.org/
[^DASH: OSM]: https://app.powerbi.com/Redirect?action=OpenReport&appId=5762de14-3aa8-4a83-92b3-045cc953e30c&reportObjectId=c8802134-4f3b-484e-bf14-1ed9f8881450&ctid=770a2450-0227-4c62-90c7-4e38537f1102&reportPage=ReportSectionf8b0041ad0335117bacb&pbi_source=appShareLink&portalSessionId=1b1f9714-60ce-413e-81c7-5a00fa7fb79f
"""

import re

from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.pandas_engine import Geometry
from pyspark.sql import functions as F

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.utils.types import PandasDataFrame


# Raw
class OSMRaw(DataFrameModel):
    """Model for OSM dataset.
    Attributes:
        layer: is the dataset layer one of [points, lines, multilinestrings, multipolygons, other_relations].
        fid: is the OSM assigned unique id.
        *tags: string tags the describe the feature, there is several managed tags, like barrier and highway, but most tags are in other_tags.
        geometry: geometries in EPSG:4326.
    """

    layer: Category = Field(isin=["points", "lines", "multilinestrings", "multipolygons", "other_relations"])
    fid: int = Field(unique=True, alias="osm_id")
    geometry: Geometry(crs="EPSG:4326") = Field()


osm_raw = SourceDataset(
    name="osm_raw",
    level0="bronze",
    level1="osm",
    model=OSMRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_openstreetmap/dataset_united_kingdom/format_PBF_united_kingdom/LATEST_united_kingdom/united-kingdom-latest.osm.pbf",
)


# Tidy
def _osm_transform(sdf: PandasDataFrame) -> PandasDataFrame:
    def _cols_to_json(row):
        cols = list(set(row.index) - {"fid", "other_tags", "geometry"})
        return {
            **{k: v for k, v in row[cols].to_dict().items() if v},
            **({k: v for k, v in re.findall(r'"([^"]+)"=>"(.*?)"', row["other_tags"])} if row["other_tags"] else {}),
        }

    return (
        sdf.withColumn("geometry", F.expr("ST_AsBinary(geometry)"))
        .mapInPandas(
            lambda pdf: pdf.assign(json_tags=pdf.apply(_cols_to_json, axis=1).astype(str))[["fid", "json_tags", "geometry"]],
            "fid:int,json_tags:string,geometry:binary",
        )
        .selectExpr(
            "fid",
            "json_tags",
            "ST_Transform(ST_GeomFromWKB(geometry), 'EPSG:27700') AS geometry",  # TODO: Check if ST_FlipCoordinates is required
        )
    )


class OSMTidy(DataFrameModel):
    """Model for OSM dataset.
    Attributes:
        fid: OSM unique id.
        tags: union all tags into one json string.
        group:
        geometry: geometries in EPSG:27700.
    """

    fid: int = Field(unique=True)
    tags: str = Field()
    group: str = Field()
    geometry: Geometry(crs=SRID) = Field()


osm_tidy = DerivedDataset(
    name="osm_tidy",
    level0="silver",
    level1="osm",
    model=OSMTidy,
    restricted=False,
    func=_osm_transform,
    dependencies=[osm_raw],
)
