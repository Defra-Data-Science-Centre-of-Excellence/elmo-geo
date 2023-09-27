import subprocess

from elmo_geo import LOG


def download_esri(url: str, path: str):
    exc = f"esri2geojson {url} {path}"
    out = subprocess.run(exc, shell=True, check=True)
    # Add to datasets
    LOG.info(out)
