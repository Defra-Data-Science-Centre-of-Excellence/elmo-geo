import os
import shutil
import zipfile
from glob import iglob

from elmo_geo import LOG
from elmo_geo.io.datasets import append_to_catalogue
from elmo_geo.io.file import convert_file
from elmo_geo.utils.dbr import spark
from elmo_geo.utils.misc import sh_run
from elmo_geo.utils.settings import FOLDER_CONDA, FOLDER_STG


def rewrite_vector_file(f_in: str) -> str:
    """For broken RPA data"""
    LOG.info("Repairing damaged vector file")
    f_tmp = "/databricks/driver/tmp.gpkg"
    sh_run(f"{FOLDER_CONDA}/ogr2ogr {f_tmp} {f_in}")
    return f_tmp


def create_outpath(f_in: str) -> str:
    """Create an output filepath based on the DASH filepath.
    dash: /dbfs/mnt/base/unrestricted/source_<source>/dataset_<dataset>_<source>/FORMAT_<format>_<source>_<dataset>/SNAPSHOT_<version>/...
    elmo: <FOLDER_STG>/<source>-<dataset>-<version>.parquet/<layer>
    """  # noqa:E501
    source = f_in.split("/source_")[1].split("/")[0]
    dataset = f_in.split("/dataset_")[1].split("/")[0]
    version = f_in.split("/SNAPSHOT_")[1].split("/")[0]
    version = version.replace("_" + dataset, "")
    dataset = dataset.replace("_" + source, "")
    return f"{FOLDER_STG}/{source}-{dataset}-{version}.parquet"


def ingest_dash(f_in: str, f_out: str = None, broken: bool = False) -> str:
    """Ingest vector data from DASH's governed/managed base area.
    Fix names, fix damaged RPA files, and convert to geoparquet in FOLDER_STG
    """
    if broken:  # RPA Spatial Mart
        f_in = rewrite_vector_file(f_in)
    if f_out is None:
        f_out = create_outpath(f_in)
    name = f_out.split("/")[-1].split(".")[0]
    convert_file(f_in, f_out)
    append_to_catalogue(
        {
            name: {
                "url": f_in,
                "filepath": f_out,
                "function": "ingest_dash",
            }
        }
    )


def search_dash(
    path: str = "/dbfs/mnt/base/unrestricted/source_*[!bluesky]/**/*.*",
    exts: tuple[str] = ("geojson", "gpkg", "shp", "gdb"),
):
    """By default this will search DASH managed data for vector files."""
    for f in iglob(path, recursive=True):
        if f.endswith(exts):
            yield f


def download_link(path: str) -> str:
    """Returns html for a download link
    Parameters:
        path: Path to the file to be downloaded, must be in the format `/dbfs/` not `dbfs:/`.
    Returns:
        html for a download link as a string to be shown using the displayHTML
            function: `displayHTML(download_link(path))`.
    Note:
        This copies the file to `FileStore` - you may want to clean up afterwards!
    """
    # filepath must be in the format `/dbfs/` not `dbfs:/`
    # Get filename
    filename = path.split("/")[-1]
    shutil.copyfile(path, f"/dbfs/FileStore/{filename}")
    # Construct download url
    url = (
        f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/files/{filename}"
        f"?o={spark.conf.get('spark.databricks.clusterUsageTags.orgId')}"
    )
    # Return html snippet
    return f"<a href={url} target='_blank'>Download file: {filename}</a>"


def extract_file(from_path: str, to_path: str):
    """Extract a zipped file from FileStore to
    a chosen location
    Parameters:
        from_path: Path of the zip file
        to_path: Directory to move the unzipped file to
    """
    filename = from_path.split("/")[-1]
    from_parent = "/".join(from_path.split("/")[:-1])
    LOG.info(f"Moving {filename} from {from_parent} to {to_path}")
    shutil.move(from_path, to_path + filename)
    LOG.info(f"Extracting {filename}")
    with zipfile.ZipFile(to_path + filename, "r") as zip_ref:
        zip_ref.extractall(to_path)
    LOG.info("Deleting zip file")
    os.remove(to_path + filename)
    LOG.info("Done!")
