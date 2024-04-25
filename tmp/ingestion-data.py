# import elm_se
# elm_se.register()
# from elm_se.io import ingest_osm

import osmnx

osmnx.settings.cache_folder = "/databricks/driver/"
osmnx.settings.timeout = 600


def ingest_osm(f, place, tags):
    gdf = osmnx.features_from_place(place, tags).reset_index()[["osmid", *tags.keys(), "geometry"]]
    gdf.to_parquet(f)
    return gdf


datasets = [
    {  # Hedgerow
        "f": "/dbfs/mnt/lab/unrestricted/elm_data/osm/hedgerow.parquet",
        "place": "England",
        "tags": {
            "barrier": ["hedge", "hedge_bank"],
            "landcover": "hedge",
        },
    },
    {  # Waterbody
        "f": "/dbfs/mnt/lab/unrestricted/elm_data/osm/waterbody.parquet",
        "place": "England",
        "tags": {
            "water": True,
            "waterway": True,
            "drain": True,
        },
    },
    {  # Heritage Wall
        "f": "/dbfs/mnt/lab/unrestricted/elm_data/osm/heritage_wall.parquet",
        "place": "England",
        "tags": {
            "wall": "dry_stone",
        },
    },
]


for dataset in datasets:
    print(dataset["tags"])
    ingest_osm(**dataset)
    print(dataset["f"])
