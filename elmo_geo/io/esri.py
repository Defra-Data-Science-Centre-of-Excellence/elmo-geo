from esridump.dumper import EsriDumper
import subprocess


def download_esri(url:str, path:str):
  exc = f'esri2geojson {url} {path}'
  out = subprocess.run(exc, shell=True, check=True)
  # Add Manifest
  LOG.info(out)



def paths_arcgis(url, batch):
  a = '/arcgis/rest/services/'
  b = '/FeatureServer/0/query?'
  f0, f1 = url.split(b)
  name = f0.split(a)[1]
  f0 += b
  f_count = f0 + 'where=1%3D1&returnCountOnly=true&f=json'
  count = requests.get(f_count).json()['count']
  paths = []
  for l in range(1, count, batch):
    u = min(l + batch, count)
    r = 'objectIds=' + ','.join( str(x) for x in range(l, u) ) + '&'
    path = f0 + r + f1
    paths.append(path)
  return paths

def read_arcgis(url, batch=200):
  '''Slowly read dataset larger than ArcGIS's batch limit.
  ArcGIS REST API defaults to limit downloading to 200 features at a time.
  This function serially reads so exceptionally large datasets may cause OutOfMemoryError.
  Distributedly reading often causes TimeoutError.
  [Example Datasets](https://github.com/aw-west-defra/cdap_geo/blob/755b89c83ccde5cd27772a2068dfacd342661f09/cdap_geo/remotes.py#L62)
  '''
  paths = paths_arcgis(url, batch)
  # for fs in paths[::BATCHSIZE // batch]:
  #   geopandas.read_file(f)
  gdf = pandas.concat(geopandas.read_file(f) for f in paths)
  return gdf

def ingest_arcgis(url):
  pass
