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
from pandera.engines.geopandas_engine import Geometry
from pyspark.sql import functions as F

from elmo_geo.etl import SRID, Dataset, DerivedDataset, SourceGlobDataset
from elmo_geo.st.geometry import load_geometry
from elmo_geo.utils.types import PandasDataFrame, SparkDataFrame


# Raw
class OSMRaw(DataFrameModel):
    """Model for OSM dataset.
    Attributes:
        layer: is the dataset layer one of [points, lines, multilinestrings, multipolygons, other_relations].
        osm_id: is the OSM assigned unique id, there is more than I64 values, so this is stored as a string.
        *tags: string tags the describe the feature, there is several managed tags, like barrier and highway, but most tags are in other_tags.
        geometry: geometries in EPSG:4326.
    """

    layer: str = Field(isin=["points", "lines", "multilinestrings", "multipolygons", "other_relations"])
    osm_id: str = Field(unique=True)
    geometry: Geometry(crs=SRID) = Field()


osm_raw = SourceGlobDataset(
    name="osm_raw",
    level0="bronze",
    level1="osm",
    model=OSMRaw,
    restricted=False,
    glob_path="/dbfs/mnt/base/unrestricted/source_openstreetmap/dataset_united_kingdom/format_PBF_united_kingdom/LATEST_united_kingdom/united-kingdom-latest.osm.pbf",
)


# Tidy
def _osm_transform(dataset: Dataset) -> SparkDataFrame:
    """Tidy the tag columns."""

    def _cols_to_dict(row: PandasDataFrame) -> dict:
        tag_cols = ["highway", "waterway", "aerialway", "barrier", "man_made", "railway"]
        return {
            **{k: v for k, v in row[tag_cols].to_dict().items() if v},
            **({k: v for k, v in re.findall(r'"([^"]+)"=>"(.*?)"', row["other_tags"])} if row["other_tags"] else {}),
        }

    def _cols_to_json(iterator):
        for pdf in iterator:
            yield pdf.assign(
                fid=lambda df: df["osm_id"],
                tags=lambda df: df.apply(_cols_to_dict, axis=1).astype(str),
            ).loc[:, ["fid", "tags", "geometry"]]

    return (
        dataset.sdf()
        .withColumn("geometry", F.expr("ST_AsBinary(geometry)"))
        .mapInPandas(_cols_to_json, "fid:string,tags:string,geometry:binary")
        .withColumn("geometry", load_geometry(subdivide=True))
    )


class OSMTidy(DataFrameModel):
    """Model for a tidier OSM dataset, with reduced precision and easier tags.

    Attributes:
        fid: OSM's unique id, but can be duplicated due to subdivide explode.
        tags: union all tags into one json string.
        geometry: geometries in EPSG:27700.
    """

    fid: str = Field(nullable=True)
    tags: str = Field(nullable=True)
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
