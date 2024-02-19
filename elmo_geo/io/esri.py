"""REST API servers designed by ESRI

Used to ingest data from sources like Natural England and Office of National Statistics.
"""

from datetime import date

import esridump  # noqa:F401
import requests

from elmo_geo.io.datasets import append_to_catalogue
from elmo_geo.io.file import convert_file
from elmo_geo.utils.misc import sh_run
from elmo_geo.utils.settings import FOLDER_STG


def get_esri_name(url):
    if "JJzESW51TqeY9uat" in url:
        source = "ne"
    elif "ESMARspQHYMw9BZ9" in url:
        source = "ons"
    dataset = url.split("/services/")[1].split("/FeatureServer")[0]
    isodate = (
        date.fromtimestamp(
            esridump.EsriDumper(url).get_metadata()["editingInfo"]["dataLastEditDate"] / 1000
        )
        .isoformat()
        .replace("-", "_")
    )
    return f"{source}-{dataset}-{isodate}"


def ingest_esri(url, name=None):
    """Download and Convert data stored on an ESRI server"""
    if name is None:
        name = get_esri_name(url)
    f_in, f_out = FOLDER_STG + f"/{name}.geojson", FOLDER_STG + f"/{name}.parquet"
    sh_run(f"esri2geojson '{url}' '{f_in}'")
    convert_file(f_in, f_out)
    append_to_catalogue(
        {
            name: {
                "url": url,
                "filepath_tmp": f_in,
                "filepath": f_out,
                "function": "ingest_esri",
            }
        }
    )


def esri_datasets(url):
    """get a dict containing all avaiable datasets"""
    r = requests.get(url + "?pjson").json()
    return dict(
        sorted(
            {
                d["name"]: {
                    "url": d["url"],
                    "function": "get_esri",
                }
                for d in r["services"]
            }.items()
        )
    )


def search_datasets(pattern, datasets):
    """Filter dictionary for keys that contain the pattern"""
    return {k: v for k, v in datasets.items() if pattern in k.lower()}
