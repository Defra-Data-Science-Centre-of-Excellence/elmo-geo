import os
import shutil
import zipfile

from elmo_geo.utils.dbr import displayHTML, spark


def zip_folder(path_in, path_out):
    with zipfile.ZipFile(path_out, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(path_in):
            for file in files:
                filepath = os.path.join(root, file)
                arcname = os.path.relpath(filepath, path_in)
                zipf.write(filepath, arcname)

def download_link(filepath: str, name: str = None, return_over_display: bool = False) -> str:
    """Create a link to download a file or folder from dbfs
    Copies a file to dbfs/FileStore
    Folders are zipped and saved to dbfs/FileStore

    example: `download_link(f)`
    """
    filepath = dbfs(filepath, False)
    if name is None:
        name = filepath.split("/")[-1]
    if os.path.isdir(filepath):
        name = f"{name}.zip"
        zip_folder(filepath, f"/dbfs/FileStore/{name}")
    else:
        shutil.copy(filepath, f"/dbfs/FileStore/{name}")
    url = "https://{workspace}/files/{name}?o={org}".format(
        workspace = spark.conf.get("spark.databricks.workspaceUrl")
        org = spark.conf.get("spark.databricks.clusterUsageTags.orgId")
    )
    if not return_over_display:
        displayHTML(f"Download: <a href={url} target='_blank'>{name}</a>")
    else:
        return url
