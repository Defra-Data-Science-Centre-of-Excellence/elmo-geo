import os
import subprocess
from glob import iglob

import geopandas as gpd

from elmo_geo import LOG
from elmo_geo.utils.misc import snake_case


def list_layers(f: str) -> list[str]:
    """List layers using Fiona, but don't fail, instead return an empty list"""
    try:
        layers = gpd.list_layers(f)["name"].tolist()
    except Exception:
        layers = []
    return layers


def list_files(f: str) -> list[str]:
    """List all the files in a directory
    similar to os.walk, but yielding full paths"""
    for f1 in iglob(f + "**", recursive=True):
        if os.path.isfile(f1):
            yield f1


def get_to_convert(f: str) -> list[tuple[str, str, str]]:
    """Get all the ogr readable files and their layers in a folder"""
    if os.path.isfile(f):
        for layer in list_layers(f):
            name = f"layer={snake_case(layer)}"
            yield f, name, layer
    else:
        f = f if f.endswith("/") else f + "/"
        for f1 in list_files(f):
            for layer in list_layers(f1):
                name = f"file={snake_case(f1.replace(f, '').split('.')[0])}/layer={snake_case(layer)}"
                yield f1, name, layer


def ogr_to_geoparquet(f_in: str, f_out: str, layer: str):
    """Convert a vector file's layer into a (Geo)Parquet file using gdal>3.5 ogr2ogr"""
    os.makedirs("/".join(f_out.split("/")[:-1]), exist_ok=True)
    out = subprocess.run(
        f"""
        export CONDA_DIR=/databricks/miniconda
        export TMPDIR=/tmp
        export OGR_GEOMETRY_ACCEPT_UNCLOSED_RING=NO
        export PROJ_LIB=$CONDA_DIR/share/proj
        $CONDA_DIR/bin/ogr2ogr -t_srs EPSG:27700 -f Parquet {f_out} {f_in} {layer}
    """,
        capture_output=True,
        text=True,
        shell=True,
    )
    LOG.info(out.__repr__())


def convert_dataset(f_in: str, f_out: str):
    """Convert a folder of vector files and all their layers into a parquet dataset"""
    for f0, part, layer in get_to_convert(f_in):
        f1 = f"{f_out}/{part}"
        ogr_to_geoparquet(f0, f1, layer)
