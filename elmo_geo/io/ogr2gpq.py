"""Convert a vector dataset to geoparquet using GDAL[^ogr2ogr].
"OGR used to stand for OpenGIS Simple Features Reference Implementation."[^faq]

[^ogr2ogr]: https://gdal.org/en/latest/programs/ogr2ogr.html
[^faq]: https://gdal.org/en/latest/faq.html
"""

import os
import subprocess
from glob import iglob

import geopandas as gpd

from elmo_geo.utils.log import LOG
from elmo_geo.utils.misc import snake_case


def list_layers(f: str) -> list[str]:
    """List layers using Fiona, but don't fail, instead return an empty list"""
    try:
        layers = gpd.list_layers(f)["name"].tolist()
    except Exception:
        layers = []
    return layers


def ogr_to_geoparquet(path_in: str, path_out: str):
    """Convert a folder or glob path of vector files and all their layers into a parquet dataset.

    BUG: multiple layers with the same name will be overwritten.
    Intentional: mergeSchema is required for datasets with different schemas.
    """
    for f_in in iglob(path_in + "**", recursive=True):
        if os.path.isfile(f_in):
            for layer in list_layers(f_in):
                f_out = f"{path_out}/layer={snake_case(layer)}/"
                os.makedirs(f_out, exist_ok=True)
                f_out += "part-0.snappy.parquet"
                LOG.info(f"ogr2ogr: {f_out}")
                out = subprocess.run(
                    f"""
                    export PATH=/databricks/miniconda/bin:$PATH
                    ogr2ogr -t_srs 'EPSG:27700' -f Parquet '{f_out}' '{f_in}' '{layer}'
                """,
                    capture_output=True,
                    text=True,
                    shell=True,
                )
                LOG.debug(out.__repr__())
