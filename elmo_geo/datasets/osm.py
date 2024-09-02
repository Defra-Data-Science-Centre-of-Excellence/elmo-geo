"""OpenStreetMaps United Kingdom (OSM), provided by DASH.

OSM is an open, crowd sourced, geographic data.
United Kingdom is a subset of osm.planet, which contains all information suitable for our domain.
This is an alternative to Ordnance Survey's Master Map.

[^OSM Map]: https://www.openstreetmap.org/
[^OSM Wiki]: https://wiki.openstreetmap.org/
[^DASH: OSM]: https://app.powerbi.com/groups/de0d7293-1d23-4194-869d-a4ff2ed2d169/reports/c8802134-4f3b-484e-bf14-1ed9f8881450?ctid=770a2450-0227-4c62-90c7-4e38537f1102&pbi_source=linkShare&bookmarkGuid=ee965c2a-c7f6-4e3b-b453-02e314252647
"""
from functools import partial

from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import SRID, DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels


class OSMRaw(DataFrameModel):
    """Model for OSM dataset.

    Attributes:
        fid: osm_id is the integer unique id for each feature.
        name: name of the feature.
        ref: ad-hoc geospatial reference to the feature.
        is_in: ad-hoc parent area.
        address: always null in the UK dataset.
        barrier, highway, place, man_made: are common tags, these are consistent.
        other_tags: is a pearl-like dictionary of other tags, these are inconsistent.
        geometry: Any type geometries in EPSG:27700.
        layer: layer name, 
    """

    fid: int = Field(coerce=True, unique=True, alias="osm_id")
    name: str = Field(coerce=True, nullable=True)
    ref: str = Field(coerce=True, nullable=True)
    is_in: str = Field(coerce=True, nullable=True)
    address: str = Field(coerce=True, nullable=True)
    barrier: str = Field(coerce=True, nullable=True)
    highway: str = Field(coerce=True, nullable=True)
    place: str = Field(coerce=True, nullable=True)
    man_made: str = Field(coerce=True, nullable=True)
    other_tags: str = Field(coerce=True, nullable=True)
    geometry: Geometry(crs=SRID) = Field(coerce=True)
    layer: str = Field(coerce)


osm_raw = SourceDataset(
    name="osm_raw",
    level0="bronze",
    level1="osm",
    model=OSMRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_openstreetmap/dataset_united_kingdom/format_PBF_united_kingdom/LATEST_united_kingdom/united-kingdom-latest.osm.pbf",
)


def tidy_osm_tags(sdf: SparkDataFrame) -> SparkDataFrame:
    return sdf.selectExpr(
        "fid",
        """LOWER(SUBSTRING(CONCAT(
            NVL(CONCAT(',"highway"=>', highway), ''),
            NVL(CONCAT(',"waterway"=>', waterway), ''),
            NVL(CONCAT(',"aerialway"=>', aerialway), ''),
            NVL(CONCAT(',"barrier"=>', barrier), ''),
            NVL(CONCAT(',"man_made"=>', man_made), ''),
            NVL(CONCAT(',"railway"=>', railway), ''),
            NVL(CONCAT(',', other_tags), '')
        ), 2)) AS tags""",
        """CASE
            WHEN (tags LIKE '%"%hedge%"=>%') THEN "hedge"
            WHEN (tags LIKE '%"%dry%wall%"=>%') THEN "dry_wall"
            WHEN (tags LIKE '%"%wall%"=>%' OR tags LIKE '%"%barrier%"=>%') THEN "wall"
            WHEN (tags LIKE '%"%waterway%"=>%') THEN "watercourse"
            WHEN (tags LIKE '%"%water%"=>%') THEN "waterbody"
            WHEN (FALSE) THEN "ditch"
            ELSE NULL
        END AS group""",
        "geometry",
    )

class OSMTidy(DataFrameModel):
    """Model for OSM with parcel dataset.

    Attributes:
        fid: integer unique id for each feature.
        tags: All tags tidied into a single columns containing JSON.
        group: Grouped tags into useful collections:
            hedgerow
            fence
            heritage_wall
            watercourse
            waterbody
        geometry: Any type geometries in EPSG:27700.
    """

    osm_id: str = Field()
    json_tags: str = Field()
    group: Category = Field(isin=["fence", "hedgerow", "heritage_wall", "watercourse", "waterbody"])
    geometry: Geometry(crs=SRID) = Field()


def fn_osm_tidy(osm):
    """Tidy OSM other_tags into JSON for readability.

    TODO: json_tags, group
    """
    sdf = osm.sdf()
    return sdf


osm_tidy = DerivedDataset(
    name="osm_tidy",
    level0="silver",
    level1="defra",
    restricted=False,
    func=fn_osm_tidy,
    dependencies=[osm_raw],
    model=OSMTidy,
)


class OSMParcel(DataFrameModel):
    """Model for OSM with parcel dataset.

    Attributes:
        id_parcel: 11 character RPA reference parcel ID (including the sheet ID) e.g. `SE12263419`.
        tags: All tags tidied into a single columns containing JSON.
        parcel: Any type geometries in EPSG:27700.
    """

    id_parcel: str = Field()
    group: str = Field()
    geometry: Geometry(crs=SRID) = Field()


osm_parcel = DerivedDataset(
    is_geo=False,
    name="osm_parcel",
    level0="silver",
    level1="osm",
    restricted=False,
    func=partial(join_parcels, columns=["group"]),
    dependencies=[reference_parcels, osm_tidy],
    model=OSMParcel,
)
