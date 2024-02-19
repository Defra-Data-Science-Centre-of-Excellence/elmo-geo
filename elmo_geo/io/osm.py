"""OpenStreetMap
Download and Ingest OSM data, there are 2 options;
1. Download a single large file: planet.osm(.pbf) or a <region>.osm(.pbf) (pbf is the compression method)
  - [planet.osm](https://planet.openstreetmap.org/)
  - regions from [Geofabrik.de](https://download.geofabrik.de/)
  - [Britain and Ireland](https://download.geofabrik.de/europe/britain-and-ireland-latest.osm.pbf)
2. Download specific tags and bounds using Overpass API
  - Try: [tag finder](https://tagfinder.osm.ch/)
"""  # noqa:E501

import osmnx  # noqa:F401

from elmo_geo.io.datasets import append_to_catalogue
from elmo_geo.io.file import convert_file
from elmo_geo.utils.misc import sh_run
from elmo_geo.utils.settings import FOLDER_STG


def ingest_osm_pbf(url, name):
    f_in, f_out = f"{FOLDER_STG}/{name}.osm.pbf", f"{FOLDER_STG}/{name}.parquet"
    sh_run(f"wget {url} -O {f_in}")
    convert_file(f_in, f_out)
    append_to_catalogue(
        {
            name: {
                "url": url,
                "filepath_tmp": f_in,
                "filepath": f_out,
                "function": "ingest_osm_pbf",
            }
        }
    )


def ingest_osm_overpass(place, tags, name):
    """Download OSM data using Overpass API, place and tags
    Try: [tag finder](https://tagfinder.osm.ch/)
    Example
    ```py
    today = datetime.now().strfmt('%Y_%m_%d')
    ingest_osm_overpass(
        place = 'England',
        tags = {'wall': 'dry_stone'},
        name = 'osm-dry_stone_wall-{today}',
    )
    ```
    """
    osmnx.settings.cache_folder = "/databricks/driver/"
    osmnx.settings.timeout = 600
    (
        osmnx.geometries_from_place(place, tags)
        .reset_index()[["osmid", *tags.keys(), "geometry"]]
        .to_crs(epsg=27700)
        .to_parquet(f"{FOLDER_STG}/{name}.parquet")
    )
