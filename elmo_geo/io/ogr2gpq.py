import os
import subprocess
from glob import iglob

from fiona import listlayers
from fiona.errors import DriverError

from elmo_geo import LOG
from elmo_geo.utils.misc import snake_case


def list_layers(f: str) -> list[str]:
    """List layers using Fiona, but don't fail, instead return an empty list"""
    try:
        layers = listlayers(f)
    except DriverError:
        layers = []
    return layers


def ogr_to_geoparquet(f_in: str, f_out: str):
    """Convert a folder or glob path of vector files and all their layers into a parquet dataset.

    BUG: multiple layers with the same name will be overwritten.
    Intentional: mergeSchema is required for datasets with different schemas.
    """
    for f0 in iglob(f_in + "**", recursive=True):
        if os.path.isfile(f0):
            for layer in list_layers(f0):
                f1 = f"{f_out}/layer={snake_case(layer)}/"
                os.makedirs(f1, exist_ok=True)
                f1 += "part-0.snappy.parquet"
                LOG.info(f"ogr2ogr: {f1}")
                out = subprocess.run(
                    f"""
                    export PATH=/databricks/miniconda/bin:$PATH
                    ogr2ogr -t_srs 'EPSG:27700' -f Parquet '{f1}' '{f0}' '{layer}'
                """,
                    capture_output=True,
                    text=True,
                    shell=True,
                )
                LOG.debug(out.__repr__())
