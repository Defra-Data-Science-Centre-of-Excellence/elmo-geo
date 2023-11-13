from elmo_geo.io.convert import to_gdf, to_sdf
from elmo_geo.io.dash import ingest_dash  # TODO: dash.extract_parquet
from elmo_geo.io.datasets import append_to_catalogue, load_sdf
from elmo_geo.io.esri import ingest_esri
from elmo_geo.io.file import to_gpq_partitioned as to_gpq
from elmo_geo.io.osm import ingest_osm_pbf
from elmo_geo.io.sentinel import download_sentinel  # TODO: ingest_sentinel


ingest_dict = {
    "ingest_dash": ingest_dash,
    "ingest_esri": ingest_esri,
    "ingest_osm_pbf": ingest_osm_pbf,
    "ingest_sentinel": download_sentinel,
}
