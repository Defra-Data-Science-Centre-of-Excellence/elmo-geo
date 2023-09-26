from esridump.dumper import EsriDumper
import subprocess


def download_esri(url:str, path:str):
  exc = f'esri2geojson {url} {path}'
  out = subprocess.run(exc, shell=True, check=True)
  # Add to datasets
  LOG.info(out)
