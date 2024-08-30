import os
import shutil
import zipfile

from elmo_geo.utils.dbr import displayHTML, spark
from elmo_geo.utils.log import LOG
from elmo_geo.utils.misc import dbfs


def zip_folder(path_in, path_out):
    LOG.info(f"Zipping directory {path_in} to {path_out}")
    with zipfile.ZipFile(path_out, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf._seekable = False  # required to avoid OSError: [Errno 95] Operation not supported
        for root, _, files in os.walk(path_in):
            for file in files:
                filepath = os.path.join(root, file)
                arcname = os.path.relpath(filepath, path_in)
                zipf.write(filepath, arcname)


def download_link(filepath: str, name: str = None, return_over_display: bool = False) -> str:
    """Create a link to download a file or folder from dbfs
    Copies a file to dbfs/FileStore
    Folders are zipped and saved to dbfs/FileStore

    example: `download_link(f)`

    TODO: fix if file is already in FileStore
    """
    filepath = dbfs(filepath, False)
    filestore = "/dbfs/FileStore"
    if name is None:
        name = filepath.split("/")[-1]
    if filepath.startswith(filestore):
        path = filepath.replace(f"{filestore}/", "")
    else:
        if os.path.isdir(filepath):
            path = f"elmo-geo-downloads/{name}.zip"
            if not os.path.exists(f"{filestore}/{path}"):
                zip_folder(filepath, f"{filestore}/{path}")
        else:
            path = f"elmo-geo-downloads/{name}"
            shutil.copy(filepath, f"{filestore}/{path}")
    workspace = spark.conf.get("spark.databricks.workspaceUrl")
    org = spark.conf.get("spark.databricks.clusterUsageTags.orgId")
    url = f"https://{workspace}/files/{path}?o={org}"
    if not return_over_display:
        displayHTML(f"Download: <a href={url} target='_blank'>{name}</a>")
    else:
        return url
