"""OpenStreetMaps United Kingdom (OSM), provided by DASH.

OSM is an open, crowd sourced, geographic data.
United Kingdom is a subset of osm.planet, which contains all information suitable for our domain.
This is an alternative to Ordnance Survey's Master Map.

[^OSM Map]: https://www.openstreetmap.org/
[^OSM Wiki]: https://wiki.openstreetmap.org/
[^DASH: OSM]: TODO
"""
from functools import partial

from pandera import DataFrameModel, Field
from pandera.dtypes import Category
from pandera.engines.pandas_engine import Geometry

from elmo_geo.etl import DerivedDataset, SourceDataset
from elmo_geo.etl.transformations import join_parcels

from .rpa_reference_parcels import reference_parcels


class OSMRaw(DataFrameModel):
    """Model for OSM dataset.

    Attributes:
        osm_id: integer unique id for each feature.
        name: name of the feature.
        ref: ad-hoc geospatial reference to the feature.
        is_in: ad-hoc parent area.
        address: always null in the UK dataset.
        barrier, highway, place, man_made: are common tags, these are consistent.
        other_tags: is a pearl-like dictionary of other tags, these are inconsistent.
        geometry: Any type geometries in EPSG:27700.
    """

    osm_id: int = Field(coerce=True, unique=True)
    name: str = Field(coerce=True, nullable=True)
    ref: str = Field(coerce=True, nullable=True)
    is_in: str = Field(coerce=True, nullable=True)
    address: str = Field(coerce=True, nullable=True)
    barrier: Category = Field(coerce=True, nullable=True, isin=["gate", "lift_gate", "bollard", "cycle_barrier", "fence"])
    highway: Category = Field(
        coerce=True,
        nullable=True,
        isin=[
            "crossing",
            "traffic_signals",
            "mini_roundabout",
            "motorway_junction",
            "speed_camera",
            "give_way",
            "turning_circle",
            "passing_place",
            "emergency_bay",
            "milestone",
            "street_lamp",
        ],
    )
    place: Category = Field(coerce=True, nullable=True, isin=["city", "village", "suburb", "town"])
    man_made: Category = Field(coerce=True, nullable=True, isin=["monitoring_station", "surveillance"])
    other_tags: str = Field(coerce=True, nullable=True)
    geometry: Geometry = Field(coerce=True)


osm_raw = SourceDataset(
    name="osm_raw",
    level0="bronze",
    level1="osm",
    model=OSMRaw,
    restricted=False,
    source_path="/dbfs/mnt/base/unrestricted/source_openstreetmap/dataset_united_kingdom/format_PBF_united_kingdom/LATEST_united_kingdom/united-kingdom-latest.osm.pbf",
)


class OSMTidy(DataFrameModel):
    """Model for OSM with parcel dataset.

    Attributes:
        osm_id: integer unique id for each feature.
        json_tags: All tags tidied into a single columns containing JSON.
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
    geometry: Geometry = Field()


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
    geometry: Geometry = Field()


osm_parcel = DerivedDataset(
    name="osm_parcel",
    level0="silver",
    level1="osm",
    restricted=False,
    func=partial(join_parcels, columns=["group"]),
    dependencies=[reference_parcels, osm_tidy],
    model=OSMParcel,
)
